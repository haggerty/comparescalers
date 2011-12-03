PROG = comparescalers

all: $(PROG)

OBJ = comparescalers.o

CFLAGS = -c `root-config --cflags`
LFLAGS = `root-config --glibs`
CXX = g++

INC = -I. 

$(PROG): $(OBJ)
	$(CXX) $(LFLAGS) $(INC) -o $(PROG) $(OBJ)

clean:
	rm -f $(PROG) *.o *.so *.d *~ *.root core

.cc.o:
	$(CXX) $(CFLAGS) $(INC) $<

.C.o:
	$(CXX) $(CFLAGS) $(INC) $<
