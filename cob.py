#! /usr/bin/env python
import py2neo
import os
import yaml
import datetime
import pandas as pd
from collections import defaultdict
from env import Env
class Cob():


    def __init__(self):
        """self.yaml_directory = yaml_directory
        self.yaml_file = yaml_file
        self.offboard_directory = offboard_directory
        self.offboard_file = offboard_file
        self.yaml_path_file = os.path.join(yaml_directory, yaml_file)
        self.offboard_path_file = os.path.join(offboard_directory, offboard_file)"""
        self.account_system_filters = ['M_TRADE', 'BR', 'CMTS', 'DABBLE', 'DB CAT', 'DBA',
                                  'DCM', 'DOMS', 'GB_SWAP', 'GES',
                                  'Global financial calculator', 'IDEAL', 'JP_SWAP',
                                  'MIS', 'MX', 'RCS', 'STR_M_TRADE', 'US_SWAP', 'XFI']

    def dataframe_to_dict(self,df):
        #takes multicolumn dataframe and returns dictionary
        mydict = defaultdict(list)
        for (key, val) in df.itertuples(index=False):
            # the str(val) conversion is specifically to create a cypher readable list
            mydict[key].append(str(val))
        return mydict

    def load_input(self):
        #TODO: Move to another class?
        #TODO: Remove all of these largely unnecessary print lines!
        print('Successfully found off-boarding input file:')
        print(self.offboard_file)
        print()

        print('The name of the input file supplied by Bonnie and Phil is:')
        print(self.offboard_file)
        print()
        df_input = pd.read_csv(self.offboard_path_file)
        #TODO: load df_input into dictionary
        print('The contents of the input file supplied by Bonnie and Phil:')
        print(df_input)

        # Offer some information on the output
        print('')
        print(df_input.describe().T)
        return df_input

    def input_cob_get_ucl_and_ucl_children(self, cobsystem, cobsystemid_list):
        # TODO: Move to another class?
        #TODO: graph argument shouldn't be here. The graph should be global once created.
        cobsystem_party = cobsystem.upper() + 'Party'
        print(cobsystem_party, cobsystemid_list)
        query = '''
        MATCH (input:''' + cobsystem_party + ''') where input.id in ''' + cobsystemid_list + '''
        and (input)-[:PRIMARY|SECONDARY]-(:UCL)
        with input
        OPTIONAL MATCH (input)-[:PRIMARY|SECONDARY]-(u:UCL)
        with u, input.id as input_cobsystemid, labels(input)[1] as input_cobsystem
        OPTIONAL MATCH (u)-[:PRIMARY|SECONDARY]-(cob)
        RETURN DISTINCT
        u.id as UCL_ID,
        input_cobsystemid,
        input_cobsystem,
        cob.id as COBSYSTEMID,
        labels(cob)[1] as COBSYSTEM

        union

        MATCH (input:''' + cobsystem_party + ''') where input.id in ''' + cobsystemid_list + '''
        and not (input)-[:PRIMARY|SECONDARY]-(:UCL)
        with input
        OPTIONAL MATCH (input)-[:PRIMARY|SECONDARY]-(u:UCL)
        with u, input.id as input_cobsystemid, labels(input)[1] as input_cobsystem
        RETURN DISTINCT
        null as UCL_ID,
        input_cobsystemid,
        input_cobsystem,
        input_cobsystemid as COBSYSTEMID,
        input_cobsystem as COBSYSTEM
        '''

        df = self.py2neo_py2_and_py3(self.graph, query)
        # print(df_result)
        return df

    def populate_cob(self, cobsystem_cobsystemid_dict):
        # TODO: Move to another class?
        """get all cob children under any UCL parent for input cobsystemid, this enables the
        collection of cobs from input and also from ucls associated with input."""
        for j, cobsystem in enumerate(cobsystem_cobsystemid_dict):
            cobsystemid_list = str(cobsystem_cobsystemid_dict[cobsystem])
            print(type(cobsystemid_list),cobsystemid_list)
            # load the first cobsystem data into a dataframe, regarless of which cobsystem it is
            if j == 0:
                df = self.input_cob_get_ucl_and_ucl_children(cobsystem, cobsystemid_list)
            else:
                # append all
                df = pd.concat([df, self.input_cob_get_ucl_and_ucl_children(cobsystem, cobsystemid_list)])
            print(cobsystem, j, cobsystemid_list, len(cobsystemid_list), df.shape)
        df.replace({'DBCATParty': 'DBCAT'}, inplace=True)
        df.drop_duplicates(inplace=True)
        ###df_dict['input_plus_ucl_cob'] = df
        # df = pd.merge(df, xdiv(cobsystem,cobsystemid_list), how = 'outer', on =['Aspen','Paragon','UCL'])
        print(df.shape, df.describe())  # df.cobsystem.unique())

        #############################################################################
        # Get COB information from the graph for each COBSYSTEM
        # use dataframe which contains the input and any UCL parent or
        ###df = df_dict['input_plus_ucl_cob']

        cobsystem_list = df.COBSYSTEM.dropna().unique()
        print('FOUND UNIQUE COBSYSTEMS: ', cobsystem_list)
        return df

    def functions(self):
        ##############################################################################
        # Create a pipeline to keep the functions and their respective parameters clean
        ##############################################################################
        # Create a dictionary of the functions defined previously
        # TODO: Remove (or replace) the reference to 'return_ucl_children_cobsys'
        # TODO: Remove the reference to 'account_graph_id_filters' once dependency is not required
        # TODO: Change the structure of code to make e.g. account_system_filters a global variable in constructor which is inherited
        function_list = {'return_crds': return_crds,
                         'return_dbclient': return_dbclient,
                         'return_dbcat': return_dbcat
                         }
        # Create dictionary of parameters inputted into the functions
        function_argument_list = {
            'return_crds': {'input_filters': input_filters},
            'return_dbclient': {'input_filters': input_filters},
            'return_dbcat': {'input_filters': input_filters}
        }


if __name__ == '__main__':
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
    """i = Cob(yaml_directory,yaml_file, offboard_directory, offboard_file)
    i.graph = i.connectToYamlGraph()
    print(i.version_of_graph())
    df_input = i.load_input()
    di = i.dataframe_to_dict(df_input[['COBSYSTEM','COBSYSTEMID']])

    i.populate_cob(di)"""
    ##############################################################################
    env_class = Env(yaml_directory,yaml_file, offboard_directory, offboard_file)
    cob_class = Cob()

    env_class.graph = env_class.connectToYamlGraph()
    cob_class.graph = env_class.graph
    print(env_class.version_of_graph())
    df_input = cob_class.load_input()
    di = cob_class.dataframe_to_dict(df_input[['COBSYSTEM','COBSYSTEMID']])

    cob_class.populate_cob(di)