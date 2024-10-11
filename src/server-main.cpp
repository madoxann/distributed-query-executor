#include "server/server.h"

int main(int argc, char** argv) {
  po::options_description desc("Allowed options");
  desc.add_options()
      ("help", "produce help message")
      ("hostname,H", po::value<std::string>()->default_value(""), "Server hostname (env: SQLFLITE_HOSTNAME)")
      ("port,R", po::value<int>()->default_value(DEFAULT_FLIGHT_PORT), "Server port")
      ("database-filename,D", po::value<std::string>()->default_value(""), "Path to database file");

  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc), vm);
  po::notify(vm);

  if (vm.count("help")) {
    std::cout << desc << "\n";
    return EXIT_SUCCESS;
  }

  fs::path database_filename = vm["database-filename"].as<std::string>();
  std::string hostname = vm["hostname"].as<std::string>();
  int port = vm["port"].as<int>();

  return run_flight_sql_server(database_filename, hostname, port);
}
