#! /usr/bin/env python
import py2neo
import os
import yaml
import datetime
import pandas as pd
from collections import defaultdict

class Env():


    def __init__(self,yaml_directory,yaml_file, offboard_directory, offboard_file):
        # Set a maximum timeout period for the graph
        py2neo.packages.httpstream.http.socket_timeout = 900

        self.yaml_directory = yaml_directory
        self.yaml_file = yaml_file
        self.yaml_path_file = os.path.join(yaml_directory, yaml_file)
        self.offboard_directory = offboard_directory
        self.offboard_file = offboard_file
        self.offboard_path_file = os.path.join(offboard_directory, offboard_file)

    def py2neo_py2_and_py3(self, graph, query):
        #TODO: Should this be in the Environment Class!?
        """
        Allows the use of various version of the py2neo library, the older
        version of py2neo required more syntax to export data from a Neo4J
        database to a pandas DataFrame

        :param query: the cypher query to be ran
        :return: Pandas DataFrame
        """

        # Older version of py2neo, more complicated syntax
        if py2neo.__version__.startswith('2.'):
            graph_result = graph.cypher.execute(query)
            return pd.DataFrame(graph_result.records, columns=graph_result.columns)
        # Newer version of py2neo, easier syntax
        elif py2neo.__version__.startswith('3.'):
            graph_result = graph.data(query)
            return pd.DataFrame(graph_result)

    def getKeyValue(self, resource, keyName):
        """
        This function is used to return configuration sensitive information stored
        in a yaml file. This function is used by the next function below

        :param resource: The file where the key is stored
        :param keyName: The name of item to be returned
        :return:
        """
        return resource[keyName] if isinstance(keyName, str) else {x: resource[x]
                                                                   for x in
                                                                   keyName}


    def getKeyValueFromYAML(self, filePath, keyName):
        """
        This function return sensitive and configuration information from yaml file
        :param filePath: path of the YAML file storing passwords
        :param keyName: Name of the key
        :return: Configuration and sensitive information
        """
        try:
            resource = open(filePath, "r")
            resourceYAML = yaml.load(resource)
            keyValues = self.getKeyValue(resourceYAML, keyName)
            resource.close()
        except (IOError, KeyError):
            keyValues = ""

        return keyValues

    def connectToYamlGraph(self):
        """
        Connect to the relevant version of the Neo4j client graph database
        """
        # Import sensitive information for the graph
        #yaml_path_file = os.path.join(yaml_directory, yaml_file)
        yaml_path_file = self.yaml_path_file
        protocol = self.getKeyValueFromYAML(yaml_path_file, 'protocol')
        url = self.getKeyValueFromYAML(yaml_path_file, 'url')
        password = self.getKeyValueFromYAML(yaml_path_file, 'password')
        new_server = self.getKeyValueFromYAML(yaml_path_file, 'new_server')
        write_graph = self.getKeyValueFromYAML(yaml_path_file, 'write_graph')
        read_graph = self.getKeyValueFromYAML(yaml_path_file, 'read_graph')
        dev_graph = self.getKeyValueFromYAML(yaml_path_file, 'dev_graph')

        # Concatenate sensitive information to create the graph address
        graph_address = protocol + ':' + url + ':' + password + '@' + new_server \
                        + ':' + write_graph
        print('The graph address is as follows:')
        print(graph_address)
        print()

        # Connect to the graph
        # TODO: Should this be in the Environment Class!?
        graph = py2neo.Graph(graph_address,bolt=False)
        print('Successfully connected to the graph')
        return graph

    def version_of_graph(self):
        # TODO: Should this be in the Environment Class!?
        query = """
        MATCH (n:GraphVersion)
        RETURN n.buildDate as buildDate,
        n.versionNumber as versionNumber
        """
        return self.py2neo_py2_and_py3(self.graph, query)

if __name__ == '__main__':
    #TODO: There should be test checks here which are more generic - to test functionality.
    z_drive_coffb = r'\\DBG.ADS.DB.COM\DUB-FSU\GROUPS\OPSOSG\GRP_DATALAB\\Analytics Team'\
                    '\Client Data Programme\Programmes\clientOffboarding'

    yaml_directory = z_drive_coffb + r'\off_boarding_script'
    yaml_file = 'configuration_settings.yaml'

    # This is the input file supplied into the Datalab. It should have 2 columns only: 'COBSYSTEM', 'COBSYSTEMID'
    offboard_directory = z_drive_coffb + r'\input_data\offboarding_files'
    offboard_file = 'kellnei_test_do_not_delete.csv'

    # This is where the output of this script will be stored
    output_file_path = z_drive_coffb + r'\offboarding_output'
    timestamp = datetime.datetime.now().strftime("_%Y_%m_%d_%H_%M")
    output_file_name = offboard_file.split('.')[0] + '_output' + timestamp
    output_file_extension = '.csv'

    ##############################################################################
    i = Env(yaml_directory,yaml_file, offboard_directory, offboard_file)
    i.graph = i.connectToYamlGraph()
    print(i.version_of_graph())