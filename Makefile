
all:	PDcodec_pb2.py

clean:
	rm -f PDcodec_pb2.py*

PDcodec_pb2.py:	PDcodec.proto
	protoc --python_out=. PDcodec.proto

