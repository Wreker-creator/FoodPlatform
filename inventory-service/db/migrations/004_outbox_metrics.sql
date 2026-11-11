CREATE INDEX idx_outbox_unpublished ON outbox (created_at) WHERE published = false;


-- This is partial index, it is the secret to our promotheus update added to the publisher, it keeps the database alive under heavy observability load

-- here is a dumb breakdown lol

-- The problem, massive sequential scans
-- with the current version of the outbox, we are never deleting values from the table, so an order that was created in fkin 1944 is still sitting in the table with with published = true
-- no one, wants anything from 1944, a world without ac's? FK NO

-- We have a new ticker (Sounds like the n word, idk why but i wouldn't say it out loud without context)
-- The ticker (pause), runs 2 queries every 5s

-- SELECT count(*) FROM outbox WHERE published = false
-- SELECT MIN(created_at) FROM outbox WHERE published = false

-- Without an index, postgres would have to scan the entire fookin table, reading all millions of rows off the hard drive every 5s 
-- just to find disappointment(2 or 3 rows that haven't been published yet)
-- That would cook my cpu man, i aint want that

-- SOLOOTION

-- By adding where published = false to the index creation, we tell postgres "Yo, check only these guys, they pay us more money(or some stuff like that)"
-- This creates a tiny seperate lookup tree that only contains unpublished rows.

-- infinitely scalable
-- self cleaning (what my mom wants me to be)
-- sorts automatically: because we indexed on "created_at", making min(created_at) an instant O(1) lookup

-- Thanks to this, prometheus queries will execute in less than 1 milisecond, safely allowing us to monitor Outbox lag in real-time without putting strain on our database (operational, mentioning this keyword to sound cool and knowledgable)

