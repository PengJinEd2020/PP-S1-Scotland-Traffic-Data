# This script is specificly created for topic 2 in the report

from functions import dealing_data
import numpy as np
import pandas as pd

def topic2_1(df):
    '''
    This function is designed to return a dataframe of total flow of each type of vehicle

    input
        df (dataframe): any given dataframe generated from "dft_rawcount_region_id_3.csv"
    '''
    def get_cp_info(df1):
        '''
        This function is used to return a series of total flow for each type of vehicle

        input
            df1 (dataframe): any given dataframe generated from "dft_rawcount_region_id_3.csv"
        '''
        # use function fix_incorrect_coord to update gven dataframe by fixing coordinates of CPs
        df1 = dealing_data.fix_incorrect_coord(df1)
        # choose basic information columns and store them in df_part
        df_part = df1[['count_point_id','latitude','longitude','pedal_cycles','cars_and_taxis',
                        'buses_and_coaches', 'lgvs', 'all_hgvs']]
        # reset type of each column in df_part
        df_part = df_part.astype({'pedal_cycles': 'int', 'cars_and_taxis': 'int',
                        'buses_and_coaches': 'int', 'lgvs': 'int', 'all_hgvs': 'int'})
        # return updated df_part by taking mean flow of each type of vehicle
        return df_part.groupby(['count_point_id','latitude','longitude']).mean().reset_index()

    # in report, only data for years 2018 and 2019 are needed
    df_18 = get_cp_info(dealing_data.pick_by_day('2018-01-01','2018-12-31',df))
    df_19 = get_cp_info(dealing_data.pick_by_day('2019-01-01','2019-12-31',df))
    # return the two dataframe
    return df_18, df_19
