# Foreman

A thin layer on top of the go nsq client to coordinate multiple handlers.

Do it like this:

```go
// Your type that needs to implement nsq.Handler
type WidgetHandler struct {}

// Responsible for constructing a nsq.Handler. You can use topic, channel and worker id if you want.
func NewWidgetHandler(topic string, channel string, id int) nsq.Handler {
  return &WidgetHandler{}
}

// The same HandleMessage that any nsq.Handler needs to implement
func (h *WidgetHandler) HandleMessage(m *nsq.Message) (err error) {
  err = errors.New("i dont know how to make widgets")
  return
}

func main() {
  // Create a new instance with a given lookupd http address
  f := foreman.New("127.0.0.1:4161")

  // Handle topic "widgets" using channel "myapp" using 10 instances of WidgetHandler.
  // Check it for errors.
  err := f.AddHandler("widgets", "myapp", 10, NewWidgetHandler)

  // You can add more handlers here. Maybe you want to process each widget twice.
  err := f.AddHandler("widgets", "myapp2", 10, NewWidgetHandler)

  // Connects to lookupd and starts processing messages.
  // Blocks, listens for signals and handles clean shutdown of all nsq readers.
  // Returns nil when all nsq readers have stopped or err if something happens during startup.
  // Check it for errors.
  err = f.Run()
}
```

# LICENSE

MIT, see LICENSE for details.
