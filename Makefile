run:
	go run main.go

create_file:
	mkdir -p data
	python3 -m create_data_file 1000000

cpu_call_graph:
	go run main.go -cpuprofile=cpuprofile.prof
	go tool pprof -pdf cpuprofile.prof