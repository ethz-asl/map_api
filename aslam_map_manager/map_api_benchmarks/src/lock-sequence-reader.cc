#include <fstream>

using namespace std;

/**
 * C++ "script" to load lock sequence measurements and plot them in gnuplot
 */
int main() {
  FILE* gnuplot = popen("gnuplot --persist", "w");
  fputs("set key off\n", gnuplot);
  fputs("plot sin(x)\n", gnuplot);
  fflush(gnuplot);
  getchar();
  ifstream file("meas_lock_sequence.txt", ios::in);
  double start, end;
  while (!file.eof()) {
    size_t rank, type;
    file >> rank >> type >> start >> end;
    fprintf(gnuplot, "set object rect from %f,%lu to %f,%lu fc rgb ", start,
            rank, end, rank + 1);
    switch (type) {
      case 1:
        fputs("\"cyan\"\n", gnuplot);
        break;
      case 2:
        fputs("\"blue\"\n", gnuplot);
        break;
      case 3:
        fputs("\"orange\"\n", gnuplot);
        break;
      case 4:
        fputs("\"red\"\n", gnuplot);
        break;
    }
    fflush(gnuplot);
  }
  fprintf(gnuplot, "set xrange [0:%f]\nset yrange [-0.1:21.1]\n", end);
  fputs("plot '-' w p\n-1 -1\ne\n", gnuplot);
}
