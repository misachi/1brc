A golang solution for https://github.com/gunnarmorling/1brc challenge

Tested with go1.21.6

## Other dependencies
  - Python v3.x.x(at least v3.6.9)
  - Make(optional)

## Run with make build tool
To create file: **make create_file**

Change the number of rows in the Makefile. Default is 1000 i.e create 1000 rows

To run program: **make run**

## Run without make
To create file: **python3 -m create_data_file `num_rows`**

`num_rows` is where you specify the row count 1 - 1,000,000,000

