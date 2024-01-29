run:
	go run main.go

create_file:
	mkdir -p data
	python3 -m create_data_file 1000000000
