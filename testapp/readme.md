# testapp

This is a sample app in flask that listen for packets on specific port.
There is a node that push data to this server.

To run it:
```
FLASK_ENV="development" FLASK_DEBUG=True FLASK_APP=app flask run --host=0.0.0.0
```
