#include "client/client.h"

#include <boost/program_options.hpp>

#include <iostream>

namespace po = boost::program_options;

int main(int argc, char** argv) {
  po::options_description desc("Allowed options");
  desc.add_options()
  ("help", "produce help message")
  ("host", po::value<std::string>()->default_value("localhost"), "Host to connect to")
  ("port", po::value<int>()->default_value(31337), "Port to connect to")
  ("query", po::value<std::string>()->default_value(""), "Query to execute");

  po::variables_map vm;
  try {
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    if (vm.count("help")) {
      std::cout << desc << "\n";
      return 0;
    }

    std::string host = vm["host"].as<std::string>();
    int port = vm["port"].as<int>();
    std::string query = vm["query"].as<std::string>();

    if (query.empty()) {
      std::cerr << "Query must be provided." << std::endl;
      return 1;
    }

    auto st = execute_sql_query(host, port, query, true);
    if (!st.ok()) {
      std::cerr << "Error: " << st.status().ToString() << std::endl;
      return 1;
    }
  } catch (const po::error& e) {
    std::cerr << "Error parsing options: " << e.what() << std::endl;
    return 1;
  }

  return 0;
}
