.PHONE: all compile run

all: compile run

compile:
	mpic++ pms.cpp -o pms

run:
	mpirun -n 10 --mca btl_tcp_if_include xx --map-by :OVERSUBSCRIBE ./pms
