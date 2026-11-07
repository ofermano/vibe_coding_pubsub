Write me a C++ pubsub code with distributer and three nodes.
Generator - randomly generages and publishes (a pointer) every second either class A or B. Both A and B has a print function that prints theier name (which the get in the ctor).
ClientA - recieve if the Generator sent class A and prints it in its own thread.
ClientB - recieve if the Generator sent class B and prints it in its own thread.

Distributer also runs in a separate thread
