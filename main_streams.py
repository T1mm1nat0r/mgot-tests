"""
03_levels_and_zones - Level & Zone Processing Service (Redis Streams)

This service:
1. Processes level achievements (gains/losses) for each bar
2. Updates zone completion states based on achievements
3. Passes bar to next service

Uses Redis Streams + Pub/Sub:
- Streams for data flow (fast, reliable)
- Pub/Sub only for processed_bar sync signal

Consumes: stream:clean_candles
Produces: stream:leveled_and_updated
"""

import os
import time
from mgot_utils import *


config = Config()

# Redis connection
r = connect_to_redis()

# Pub/Sub for processed_bar sync (kept for backward compatibility)
pub = r.pubsub()
pub.subscribe('processed_bar')

# Stream configuration
STREAM_IN = 'stream:clean_candles'
STREAM_OUT = 'stream:leveled_and_updated'
GROUP = '03_levels_and_zones'
CONSUMER = f'{GROUP}-{os.getpid()}'


# =============================================================================
# LEVEL ACHIEVEMENT PROCESSING
# =============================================================================

def process_level_achievements(bar: Bar, r) -> tuple[list[str], list[Level]]:
    """Process level achievements for a bar."""
    pipe = r.pipeline()
    
    time_past_bar = bar.time_of_previous_bar(1)
    current_time = bar.time

    lost_lvl_ids, gained_lvl_ids = bar.identify_achievements(r)
    
    if not lost_lvl_ids and not gained_lvl_ids:
        return [], []
    
    lost_levels = Level.fetch_many(lost_lvl_ids, pipe) if lost_lvl_ids else []
    gained_levels = Level.fetch_many(gained_lvl_ids, pipe) if gained_lvl_ids else []

    record_level_events(lost_levels, gained_levels, time_past_bar, current_time, pipe)
    update_tracking_lists(bar, lost_levels, gained_levels, pipe)
    pipe.execute()
    
    log_consecutive_events(lost_levels, gained_levels, current_time)
    
    zone_ids = collect_affected_zones(lost_levels, gained_levels)
    all_modified_levels = lost_levels + gained_levels
    return zone_ids, all_modified_levels


def record_level_events(lost_levels: list[Level], gained_levels: list[Level], 
                        time_past_bar: int, current_time: int, pipe) -> None:
    for lvl in lost_levels:
        lvl.record_loss(time_past_bar, current_time)
        lvl.sync_with_db(pipe)
    
    for lvl in gained_levels:
        lvl.record_gain(time_past_bar, current_time)
        lvl.sync_with_db(pipe)


def update_tracking_lists(bar: Bar, lost_levels: list[Level], 
                          gained_levels: list[Level], pipe) -> None:
    for lvl in lost_levels:
        lvl.transfer_tracking(pipe, bar.symbol, bar.timeframe, 
                              'to_lose', 'to_gain', lvl.should_stop_tracking_losses())
    
    for lvl in gained_levels:
        lvl.transfer_tracking(pipe, bar.symbol, bar.timeframe, 
                              'to_gain', 'to_lose', lvl.should_stop_tracking_gains())


def log_consecutive_events(lost_levels: list[Level], gained_levels: list[Level], 
                           current_time: int) -> None:
    for lvl in lost_levels:
        lvl.log_event('loss', current_time)
    for lvl in gained_levels:
        lvl.log_event('gain', current_time)


def collect_affected_zones(lost_levels: list[Level], gained_levels: list[Level]) -> list[str]:
    zone_ids = set()
    for lvl in lost_levels:
        zone_ids.add(lvl.zone_id)
    for lvl in gained_levels:
        zone_ids.add(lvl.zone_id)
    return list(zone_ids)


# =============================================================================
# ZONE UPDATE PROCESSING
# =============================================================================

def process_zone_updates(bar: Bar, affected_zones: list[str], r) -> list[Zone]:
    if not affected_zones:
        return []
    
    pipe = r.pipeline()
    zones = Zone.fetch_many(affected_zones, pipe) if affected_zones else []
    
    for zone in zones:
        post_process_zone(bar, zone, r)
    
    return zones


# =============================================================================
# SYNCHRONIZATION
# =============================================================================

def sync_with_achiever(bar: Bar) -> None:
    """Wait for pipeline to finish processing this bar."""
    for message in pub.listen():
        if message['type'] != 'message':
            continue
        
        data = message['data']
        if isinstance(data, bytes):
            data = data.decode('utf-8')
        
        if data == bar.id:
            parts = data.split(':')
            symbol, timeframe, bar_time = parts[0], parts[1], parts[-1]
            print(f"processed: {symbol} | {timeframe} | {convert_epoch_to_local(bar_time)}")
            break


# =============================================================================
# MAIN CONSUMER
# =============================================================================

def process_bar(value: dict):
    """Process a single bar through the level/zone pipeline."""
    t_start = time.perf_counter()
    bar = Bar.initiate_bar(value)
    t_init = time.perf_counter()
    
    # Step 1: Process level achievements
    affected_zone_ids, modified_levels = process_level_achievements(bar, r)
    t_achievements = time.perf_counter()
    
    # Store achievements on bar
    pipe = r.pipeline()
    bar.achievements = ', '.join(affected_zone_ids) if affected_zone_ids else ''
    bar.sync_with_db(pipe)
    pipe.execute()
    t_bar_sync = time.perf_counter()
    
    # Step 2: Update zone states
    modified_zones = []
    if affected_zone_ids:
        modified_zones = process_zone_updates(bar, affected_zone_ids, r)
    t_zones = time.perf_counter()
    
    # Log state changes for event sourcing
    if modified_zones or modified_levels:
        log_state_snapshot(r, bar, modified_zones, modified_levels)
    t_log = time.perf_counter()
    
    # Step 3: Produce to next service via Redis Stream
    produce(r, STREAM_OUT, bar.model_dump(mode='json'))
    t_produce = time.perf_counter()
    
    # Wait for pipeline to complete
    sync_with_achiever(bar)
    t_sync_wait = time.perf_counter()
    
    print(f'[03 TIMING] {bar.timeframe} | init:{(t_init-t_start)*1000:.1f}ms | achv:{(t_achievements-t_init)*1000:.1f}ms | bar_sync:{(t_bar_sync-t_achievements)*1000:.1f}ms | zones:{(t_zones-t_bar_sync)*1000:.1f}ms | log:{(t_log-t_zones)*1000:.1f}ms | produce:{(t_produce-t_log)*1000:.1f}ms | WAIT:{(t_sync_wait-t_produce)*1000:.1f}ms | TOTAL:{(t_sync_wait-t_start)*1000:.1f}ms')


@stream_consumer(r, STREAM_IN, GROUP, CONSUMER)
def main(value: dict):
    """Main consumer loop."""
    process_bar(value)


if __name__ == "__main__":
    print(f"03_levels_and_zones starting...")
    print(f"Listening on: {STREAM_IN}")
    print(f"Producing to: {STREAM_OUT}")
    main()
