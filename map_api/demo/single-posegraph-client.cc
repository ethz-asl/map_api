/*
 * single-posegraph-client.cc
 *
 *  Created on: Mar 24, 2014
 *      Author: titus
 */

#include <iostream>
#include <string>
#include <fstream>
#include <vector>
#include <sstream>

#include <Poco/MD5Engine.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "map-api/posegraph/edge-table.h"
#include "map-api/posegraph/edge.h"
#include "map-api/posegraph/frame-table.h"
#include "map-api/posegraph/frame.h"

// mission file flag
DEFINE_string(mission_file, "",
              "The csv file specifying the mission to process");

// global frame, edge table
map_api::posegraph::EdgeTable edgeTable;
map_api::posegraph::FrameTable frameTable;

// only for visualization purposes
std::vector<map_api::posegraph::Edge> edges;
FILE* gnuplot;

map_api::Hash globalFrame;

void addFrame(const double timestamp,
              const std::vector<double> &transformVals,
              const std::string &image){
  if (transformVals.size() != 12){
    LOG(FATAL) << "Passed transform size isn't 12!";
  }
  // create frame TODO(tcies) constructors for these kinds of things
  map_api::posegraph::Frame frame;
  frame.set_time(timestamp); // TODO(tcies) should be an int64
  frame.set_dataformat("naive");
  frame.set_dataref(image);
  map_api::Hash current = frameTable.insertFrame(frame);

  // create edge from global
  map_api::posegraph::Edge edge;
  edge.set_confidence(1);
  edge.set_from(globalFrame.getString());
  edge.set_to(current.getString());
  for (const double v : transformVals){
    edge.add_transform(v);
  }

  edgeTable.insertEdge(edge, frameTable);
  edges.push_back(edge);
}

void plotAllEdges(){
  fprintf(gnuplot, "plot '-' w l\n");
  for (const map_api::posegraph::Edge& edge : edges){
    fprintf(gnuplot, "%lf %lf\n",edge.transform(3), edge.transform(7));
  }
  fprintf(gnuplot, "e\n");
  fflush(gnuplot);
}

int main(int argc, char* argv[]){
  // parameters
  const int num_skip_frames = 100;

  // read command line args
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  CHECK_NE(FLAGS_mission_file, "") <<
      "Please pass a mission CSV file as argument";

  // launch edge and frame table
  edgeTable.init();
  frameTable.init();
  // create global anchor frame
  map_api::posegraph::Frame frame;
  // TODO(tcies) protobuf serialization and subsequent passage of blob to
  // sqlite doesn't take well to "0" doubles!
  frame.set_time(0.1);
  frame.set_dataformat("anchor");
  frame.set_dataref("");
  globalFrame = frameTable.insertFrame(frame);

  // launch gnuplot
  gnuplot = popen("gnuplot --persist","w");

  // open mission file
  // assumed format: image_number, timestamp, time-related scalar,
  // 3x4 transformation matrix
  // expressing the orientation of the imu frame of reference expressed in the
  // global (world) frame of reference, separated by tabs
  std::ifstream in(FLAGS_mission_file);
  std::string line;
  int i = 0;
  for (getline(in, line); !in.eof(); getline(in, line)){
    if (!(i%num_skip_frames)){
      std::istringstream iss(line);
      std::string image;
      double timestamp, fakeHeight;
      std::vector<double> transformVals(12,0);
      iss >> image >> timestamp >> fakeHeight;
      for (double &v : transformVals){
        iss >> v;
      }
      addFrame(timestamp, transformVals, image);
      plotAllEdges();
    }
    i++;
  }
}
