#! /usr/bin/env python
class Environment():
    import py2neo
    import os
    import yaml
    import pandas as pd

    def __init__(self,yaml_directory,yaml_file, offboard_directory, offboard_file):
        # Set a maximum timeout period for the graph
        self.py2neo.packages.httpstream.http.socket_timeout = 900

        self.yaml_directory = yaml_directory
        self.yaml_file = yaml_file
        self.offboard_directory = offboard_directory
        self.offboard_file = offboard_file
        self.yaml_path_file = self.os.path.join(yaml_directory, yaml_file)
        self.offboard_path_file = self.os.path.join(offboard_directory, offboard_file)

    def py2neo_py2_and_py3(self, graph, query):
        """
        Allows the use of various version of the py2neo library, the older
        version of py2neo required more syntax to export data from a Neo4J
        database to a pandas DataFrame

        :param query: the cypher query to be ran
        :return: Pandas DataFrame
        """

        # Older version of py2neo, more complicated syntax
        if self.py2neo.__version__.startswith('2.'):
            graph_result = graph.cypher.execute(query)
            return self.pd.DataFrame(graph_result.records, columns=graph_result.columns)
        # Newer version of py2neo, easier syntax
        elif self.py2neo.__version__.startswith('3.'):
            graph_result = graph.data(query)
            return self.pd.DataFrame(graph_result)

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
            resourceYAML = self.yaml.load(resource)
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
        graph = self.py2neo.Graph(graph_address)
        print('Successfully connected to the graph')
        return graph

    def version_of_graph(self, graph):
        query = """
        MATCH (n:GraphVersion)
        RETURN n.buildDate as buildDate,
        n.versionNumber as versionNumber
        """
        return self.py2neo_py2_and_py3(graph, query)

    def load_input(self):
        #TODO: Remove all of these largely unnecessary print lines!
        print('Successfully found off-boarding input file:')
        print(self.offboard_file)
        print()

        print('The name of the input file supplied by Bonnie and Phil is:')
        print(self.offboard_file)
        print()
        df_input = self.pd.read_csv(self.offboard_path_file)
        # TODO: load df_input into dictionary
        print('The contents of the input file supplied by Bonnie and Phil:')
        print(df_input)

        # Offer some information on the output
        print('')
        print(df_input.describe().T)
        return df_input


if __name__ == '__main__':
    z_drive_coffb = r'\\DBG.ADS.DB.COM\DUB-FSU\GROUPS\OPSOSG\GRP_DATALAB\\Analytics Team'\
                    '\Client Data Programme\Programmes\clientOffboarding'

    yaml_directory = z_drive_coffb + r'\off_boarding_script'
    yaml_file = 'configuration_settings.yaml'

    # This is the input file supplied into the Datalab. It should have 2 columns only: 'COBSYSTEM', 'COBSYSTEMID'
    offboard_directory = z_drive_coffb + r'\input_data\offboarding_files'
    offboard_file = 'kellnei_test_do_not_delete.csv'

    ##############################################################################
    i = Environment(yaml_directory,yaml_file, offboard_directory, offboard_file)
    graph = i.connectToYamlGraph()
    print(i.version_of_graph(graph))
    df_input = i.load_input()
    print(df_input.describe())