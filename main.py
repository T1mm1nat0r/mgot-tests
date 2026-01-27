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

Reset Handling:
- Listens for PIPELINE_RESET signal on processed_bar channel
- Checks ingestion mode periodically during sync wait
- Automatically recovers when processing is paused/stopped/reset
"""

import os
import time
from mgot_utils import *


config = Config()

# Redis connection
r = connect_to_redis()

# Pub/Sub for processed_bar sync
pub = r.pubsub()
pub.subscribe('processed_bar')

# Stream configuration
STREAM_IN = 'stream:clean_candles'
STREAM_OUT = 'stream:leveled_and_updated'
GROUP = '03_levels_and_zones'
CONSUMER = f'{GROUP}-{os.getpid()}'

# Sync configuration
SYNC_TIMEOUT_SECONDS = 5  # Check for mode changes every 5 seconds
MAX_SYNC_WAIT_SECONDS = 60  # Maximum time to wait for sync before giving up


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
        
        # Check if this is a consolidation level - mark consolidation as broken
        if 'consolidation' in lvl.zone_id:
            _handle_consolidation_level_loss(lvl, current_time, pipe)
    
    for lvl in gained_levels:
        lvl.record_gain(time_past_bar, current_time)
        lvl.sync_with_db(pipe)


def _handle_consolidation_level_loss(level: Level, loss_time: int, pipe) -> None:
    """
    Handle when a consolidation level is lost - marks the consolidation breakout.
    
    When block_zero (support) is lost = bearish breakout
    When block_one (resistance) is lost = bullish breakout
    """
    from mgot_utils.models import Consolidation
    
    consolidation = Consolidation.fetch_by_id(level.zone_id, r)
    if not consolidation:
        return
    
    # Only process if consolidation is confirmed (not already complete)
    if consolidation.completion != 'confirmed':
        return
    
    # Determine breakout direction based on which level was lost
    if level.name == 'block_zero':  # Support lost = bearish
        consolidation.direction = 0
        consolidation.broken_level = 'support'
    elif level.name == 'block_one':  # Resistance lost = bullish
        consolidation.direction = 1
        consolidation.broken_level = 'resistance'
    else:
        return
    
    # Mark as broken
    consolidation.level_lost = 1  # Use int for Redis compatibility
    consolidation.level_lost_time = loss_time
    consolidation.sync_with_db(pipe)
    
    print(f'[Consolidation] Level loss detected | {level.zone_id} | '
          f'Level: {level.name} | Direction: {"UP" if consolidation.direction == 1 else "DOWN"}')


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

def get_ingestion_mode(symbol: str) -> str:
    """Get current ingestion mode from Redis."""
    status_key = f"ingestion:{symbol}:status"
    mode = r.hget(status_key, "mode")
    return mode if mode else "stopped"


def clear_stale_messages():
    """Clear any stale messages from the pub/sub channel."""
    while True:
        msg = pub.get_message(timeout=0.1)
        if msg is None:
            break


def sync_with_achiever(bar: Bar) -> bool:
    """
    Wait for pipeline to finish processing this bar.

    Returns:
        True if sync completed successfully
        False if interrupted (pause/stop/reset)
    """
    start_time = time.time()

    while True:
        elapsed = time.time() - start_time

        # Check if we've waited too long
        if elapsed > MAX_SYNC_WAIT_SECONDS:
            print(f"[{bar.symbol}:{bar.timeframe}] Sync timeout after {MAX_SYNC_WAIT_SECONDS}s - skipping bar")
            return False

        # Non-blocking check for messages with timeout
        message = pub.get_message(timeout=SYNC_TIMEOUT_SECONDS)

        if message is None:
            # Timeout - check if mode changed
            mode = get_ingestion_mode(bar.symbol)
            if mode in ['stopped', 'paused']:
                print(f"[{bar.symbol}:{bar.timeframe}] Sync interrupted - mode is {mode}")
                return False
            continue

        if message['type'] != 'message':
            continue

        data = message['data']
        if isinstance(data, bytes):
            data = data.decode('utf-8')

        # Check for pipeline reset signal
        if data == 'PIPELINE_RESET':
            print(f"[{bar.symbol}:{bar.timeframe}] Pipeline reset detected - clearing sync state")
            clear_stale_messages()
            return False

        # Check if this is our bar
        if data == bar.id:
            parts = data.split(':')
            symbol, timeframe, bar_time = parts[0], parts[1], parts[-1]
            print(f"processed: {symbol} | {timeframe} | {convert_epoch_to_local(bar_time)}")
            return True

        # Check if this is a newer bar (we missed our sync, skip ahead)
        try:
            parts = data.split(':')
            if len(parts) >= 4:
                msg_symbol, msg_tf, _, msg_time = parts[0], parts[1], parts[2], int(parts[3])
                if msg_symbol == bar.symbol and msg_tf == bar.timeframe:
                    if msg_time > bar.time:
                        print(f"[{bar.symbol}:{bar.timeframe}] Received newer bar sync - skipping stale bar")
                        return False
        except (ValueError, IndexError):
            pass


# =============================================================================
# MAIN CONSUMER
# =============================================================================

def process_bar(value: dict) -> bool:
    """
    Process a single bar through the level/zone pipeline.

    Returns:
        True if processing completed successfully
        False if interrupted (caller should handle recovery)
    """
    bar = Bar.initiate_bar(value)

    # Check mode before processing
    mode = get_ingestion_mode(bar.symbol)
    if mode in ['stopped', 'paused']:
        print(f"[{bar.symbol}:{bar.timeframe}] Skipping bar - mode is {mode}")
        return False

    # Step 1: Process level achievements
    affected_zone_ids, modified_levels = process_level_achievements(bar, r)

    # Store achievements on bar
    pipe = r.pipeline()
    bar.achievements = ', '.join(affected_zone_ids) if affected_zone_ids else ''
    bar.sync_with_db(pipe)
    pipe.execute()

    # Step 2: Update zone states
    modified_zones = []
    if affected_zone_ids:
        modified_zones = process_zone_updates(bar, affected_zone_ids, r)

    # Log state changes for event sourcing
    if modified_zones or modified_levels:
        log_state_snapshot(r, bar, modified_zones, modified_levels)

    # Step 3: Produce to next service via Redis Stream
    produce(r, STREAM_OUT, bar.model_dump(mode='json'))

    # Wait for pipeline to complete (with timeout and mode checking)
    sync_success = sync_with_achiever(bar)

    if not sync_success:
        # Sync was interrupted - clear any stale pub/sub messages
        clear_stale_messages()

    return sync_success


@stream_consumer(r, STREAM_IN, GROUP, CONSUMER)
def main(value: dict):
    """Main consumer loop."""
    process_bar(value)


if __name__ == "__main__":
    print(f"03_levels_and_zones starting...")
    print(f"Listening on: {STREAM_IN}")
    print(f"Producing to: {STREAM_OUT}")
    print(f"Sync timeout: {SYNC_TIMEOUT_SECONDS}s, Max wait: {MAX_SYNC_WAIT_SECONDS}s")
    main()
