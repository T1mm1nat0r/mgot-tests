# 03_leveler_and_updater

Merged service combining the functionality of:
- `03_leveler`: Process level achievements (gains/losses)
- `04_zone_updater`: Update zone completion states

## Flow

1. Receives bars from `clean_candles` topic
2. Identifies which levels were gained or lost
3. Records achievements on levels
4. Updates zone completion states
5. Sends bar to `leveled_and_updated` topic

## Benefits

- Eliminates 1 Kafka topic hop between services
- Reduces latency by ~10-20ms per bar
- Single Redis connection for both operations
