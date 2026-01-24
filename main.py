"""
03_levels_and_zones - Merged service: 03_leveler + 04_zone_updater

This service:
1. Processes level achievements (gains/losses) for each bar
2. Updates zone completion states based on achievements
3. Passes bar to peaks finder

Optimized for speed with:
- Fresh Redis pipelines per operation
- Batch Redis operations
- Optimized Kafka settings
"""

import os
from confluent_kafka import Producer, Consumer
from mgot_utils import *


config = Config()

# Redis - single connection, fresh pipelines per operation
r = connect_to_redis()
pub = r.pubsub()
pub.subscribe('processed_bar')

# Kafka - optimized settings for throughput
bootstrap = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'redpanda:9092')
producer_conf = {
    'bootstrap.servers': bootstrap,
    'client.id': '03_levels_and_zones-producer',
    'linger.ms': 5,
    'batch.num.messages': 100,
    'queue.buffering.max.kbytes': 32768,
}
consumer_conf = {
    'bootstrap.servers': bootstrap,
    'group.id': '03_levels_and_zones-group',
    'auto.offset.reset': 'earliest',
    'fetch.min.bytes': 1024,
    'fetch.wait.max.ms': 50,
}

producer = Producer(producer_conf)
consumer = Consumer(consumer_conf)
consumer.subscribe(['clean_candles'])


# =============================================================================
# LEVEL ACHIEVEMENT PROCESSING (from 03_leveler)
# =============================================================================

def process_level_achievements(bar: Bar, r) -> list[str]:
    """
    Process level achievements for a bar and return list of affected zone IDs.
    """
    pipe = r.pipeline()
    
    time_past_bar = bar.time_of_previous_bar(1)
    current_time = bar.time

    # Identify which levels were breached
    lost_lvl_ids, gained_lvl_ids = bar.identify_achievements(r)
    
    # Early exit if no achievements
    if not lost_lvl_ids and not gained_lvl_ids:
        return []
    
    # Fetch level objects in batch
    lost_levels = Level.fetch_many(lost_lvl_ids, pipe) if lost_lvl_ids else []
    gained_levels = Level.fetch_many(gained_lvl_ids, pipe) if gained_lvl_ids else []

    # Record events and persist
    record_level_events(lost_levels, gained_levels, time_past_bar, current_time, pipe)
    
    # Update tracking lists
    update_tracking_lists(bar, lost_levels, gained_levels, pipe)
    
    # Execute all Redis commands in single round-trip
    pipe.execute()
    
    # Log consecutive events
    log_consecutive_events(lost_levels, gained_levels, current_time)
    
    # Collect affected zone IDs
    return collect_affected_zones(lost_levels, gained_levels)


def record_level_events(lost_levels: list[Level], gained_levels: list[Level], 
                        time_past_bar: int, current_time: int, pipe) -> None:
    """Record loss/gain events on levels and sync to database."""
    for lvl in lost_levels:
        lvl.record_loss(time_past_bar, current_time)
        lvl.sync_with_db(pipe)
    
    for lvl in gained_levels:
        lvl.record_gain(time_past_bar, current_time)
        lvl.sync_with_db(pipe)


def update_tracking_lists(bar: Bar, lost_levels: list[Level], 
                          gained_levels: list[Level], pipe) -> None:
    """Update Redis sorted sets that track which levels to monitor."""
    for lvl in lost_levels:
        lvl.transfer_tracking(pipe, bar.symbol, bar.timeframe, 
                              'to_lose', 'to_gain', lvl.should_stop_tracking_losses())
    
    for lvl in gained_levels:
        lvl.transfer_tracking(pipe, bar.symbol, bar.timeframe, 
                              'to_gain', 'to_lose', lvl.should_stop_tracking_gains())


def log_consecutive_events(lost_levels: list[Level], gained_levels: list[Level], 
                           current_time: int) -> None:
    """Log levels that had consecutive loss/gain events."""
    for lvl in lost_levels:
        lvl.log_event('loss', current_time)
    
    for lvl in gained_levels:
        lvl.log_event('gain', current_time)


def collect_affected_zones(lost_levels: list[Level], gained_levels: list[Level]) -> list[str]:
    """Collect unique zone IDs from all affected levels."""
    zone_ids = set()
    for lvl in lost_levels:
        zone_ids.add(lvl.zone_id)
    for lvl in gained_levels:
        zone_ids.add(lvl.zone_id)
    return list(zone_ids)


# =============================================================================
# ZONE UPDATE PROCESSING (from 04_zone_updater)
# =============================================================================

def process_zone_updates(bar: Bar, affected_zones: list[str]) -> None:
    """
    Post-process zones that were affected by this bar's level achievements.
    Only processes zones that were actually affected.
    """
    if not affected_zones:
        return
    
    pipe = r.pipeline()
    
    # Fetch only affected zones in batch
    zones = Zone.fetch_many(affected_zones, pipe) if affected_zones else []
    
    # Post-process each zone (updates completion state)
    for zone in zones:
        post_process_zone(bar, zone)


# =============================================================================
# SYNCHRONIZATION
# =============================================================================

def sync_with_achiever(bar: Bar) -> None:
    """
    Wait for the rest of the pipeline to finish processing this bar.
    """
    for message in pub.listen():
        if message['type'] != 'message':
            continue
        
        data = message['data']
        if isinstance(data, bytes):
            data = data.decode('utf-8')
        
        if data == bar.id:
            parts = data.split(':')
            symbol, timeframe, time = parts[0], parts[1], parts[-1]
            print(f"processed: {symbol} | {timeframe} | {convert_epoch_to_local(time)}")
            break


# =============================================================================
# MAIN CONSUMER LOOP
# =============================================================================

@kafka_consumer(consumer, producer) 
def main(value, producer):
    bar = Bar.initiate_bar(value)
    
    # Step 1: Process level achievements (was 03_leveler)
    affected_zones = process_level_achievements(bar, r)
    
    # Store achievements on bar - use fresh pipeline
    pipe = r.pipeline()
    bar.achievements = ', '.join(affected_zones) if affected_zones else ''
    bar.sync_with_db(pipe)
    pipe.execute()
    
    # Step 2: Update zone states (was 04_zone_updater) - only if zones affected
    if affected_zones:
        process_zone_updates(bar, affected_zones)
    
    # Step 3: Send to next service (04_peaks_and_structures)
    try:
        produce_with_retry(producer, 'leveled_and_updated', bar.model_dump_json(), key=bar.id)
    except Exception as e:
        print(f'Produce error: {e}')
    
    # Wait for pipeline to complete
    sync_with_achiever(bar)


if __name__ == "__main__":
    main()
