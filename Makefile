.PHONY: build run client test send receive kill

# Build the broker binary.
build:
	go build -o broker .

# Run the broker in the foreground (Ctrl-C to stop).
run: build
	./broker

# Run the progressive test client against a running broker.
# (Start the broker with `make run` in another terminal first.)
client:
	cd test && go run .

# Producer / consumer demo (broker must be running via `make run`).
# Order matters in v1: `make send` first, then `make receive`.
#   make send MSG="your message"
send:
	cd test && go run ./send $(MSG)

receive:
	cd test && go run ./receive

# One-shot: build broker, run it in the background, run the client, then stop it.
test: build
	@./broker > /tmp/broker.log 2>&1 & echo $$! > /tmp/broker.pid; \
	sleep 0.5; \
	cd test && go run . ; status=$$?; \
	kill `cat /tmp/broker.pid` 2>/dev/null; \
	echo "--- broker log ---"; cat /tmp/broker.log; \
	exit $$status

# Kill any stray broker listening on 5672 (ours, IPv6).
kill:
	@pkill -f '^./broker' 2>/dev/null || true
