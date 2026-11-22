
CREATE TABLE processed_events(
    event_id VARCHAR(255) PRIMARY KEY,
    process_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- the reason for event_id being varchar is simple, take something like order service for example, which has more than 1 consumer
-- now it listens to both inventory and payment events, if both the services sent outboxes with the same outbox_id, a clash will happen

-- this is event idempotency basically, we are adding this because in certain services such as notification where we can't rely on 
-- state transitions this acts as steady protection