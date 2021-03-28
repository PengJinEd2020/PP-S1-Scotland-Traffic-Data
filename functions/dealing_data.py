# This script is aimed to implement specific opration when given a dataframe

import pandas as pd

def pick_by_city(cityname, df):
    '''
    This function is used to select all rows containing appointed city name

    input
        cityname (string): should be key word of a city name
        df (dataframe): any dataframe generated from "dft_rawcount_region_id_3.csv"
    output
        df_city (dataframe): dataframe includes all rows containing appointed city name
    '''
    # all rows containing appointed city name are stored in dataframe df_city
    df_city = df.loc[df['local_authority_name'].str.contains(cityname, case=False)]
    # return the new dataframe
    return df_city


def pick_by_day(start_date, end_date, df):
    '''
    This function is used to select all rows related to dates from start_date to end_date
    and form of start_date and end_date should follow the style 'yyyy-mm-dd'

    input
        start_date (string): appoited start date
        end_date (string): appointed end date
        df (dataframe): any dataframe generated from "dft_rawcount_region_id_3.csv"
    output
        dfnew (dataframe): includes all rows related to dates from start_date to end_date
    '''
    # cast type of 'count_date' to 'datetime64'
    df = df.astype({'count_date': 'datetime64'})
    # copy rows with date between start_date and end_date to new dataframe dfnew
    dfnew = df[(df['count_date']>=start_date) & (df['count_date']<=end_date)].copy()
    # return the new dataframe
    return dfnew

def split_by_road(df, split_by = 'UC'):
    '''
    This function is used to split one dataframe into two. When split_by='UC',
    the output two dataframes are: one without road_name 'U','C' and the other with;
    when split_by='Mm', the output two dataframes are: one including major roads
    and the other including minor roads

    input
        df (dataframe): any dataframe generated from "dft_rawcount_region_id_3.csv"
        split_by (string, default 'UC'): method or criteria to split the dataframe
    output
        df_ex_UC (dataframe): dataframe without 'U/C' roads
        df_UC (dataframe): dataframe including 'U/C' roads

        df_M (dataframe): dataframe including Major roads
        df_m (dataframe): dataframe including Minor roads
    '''
    # when the method is defined as 'UC'
    if split_by == 'UC':
        # select all rows including 'U' and collect them to df_U
        df_U = df[df['road_name'].isin(['U'])]
        # select all rows including 'C' and collect them to df_C
        df_C = df[df['road_name'].isin(['C'])]
        # put the two dataframe df_U and df_C together and rename it as df_UC
        df_UC = df_U.append(df_C, ignore_index=True)

        # collect rows whose value in 'road_name' column is neither 'U' nor 'C'
        df_ex_UC1 = df[~(df['road_name'].isin(['U']))]
        df_ex_UC2 = df_ex_UC1[~(df_ex_UC1['road_name'].isin(['C']))]
        # rename the final dataframe as df_ex_UC
        df_ex_UC = df_ex_UC2.reset_index(drop = True)

        # return the two dataframe: one without road_name 'U','C' and the other with
        return df_ex_UC, df_UC

    # when the method is defined as 'Mm'
    if split_by == 'Mm':
        # difine dataframe df_M to collect rows with Major road_type
        df_M = df[df['road_type'] == 'Major']
        # difine dataframe df_m to collect rows with Minor road_type
        df_m = df[df['road_type'] == 'Minor']
        # return the two dataframes: one including major and the other including minor
        return df_M, df_m

def add_col(df, add=None):
    '''
    This function is used to add a column named total flow of bicycles and all_motor_vehicles

    input
        df (dataframe): any dataframe generated from "dft_rawcount_region_id_3.csv"
        add (string, default None): if add=None, return total flow and if add='flow_per_km',
        return total flow in each single kilometer
    output
        df2 (dataframe): dataframe of total flow of bicycles and all_motor_vehicles

        df3 (dataframe): dataframe of total flow of bicycles and all_motor_vehicles
        in each single kilometer
    '''
    # use dataframe df2 to store total flow(sum) of bicycles and all_motor_vehicles
    df2 = df.copy()
    df2.loc[:,'total_flow'] = df.loc[:,'pedal_cycles'] + df.loc[:,'all_motor_vehicles']

    if add == 'flow_per_km':
        # when add='flow_per_km', total flow should be divided by corresponding link_length_km
        # to get the flow in each kilometer
        df3 = df2.copy()
        df3.loc[:,'flow_per_km'] = df2.loc[:,'total_flow'] / df2.loc[:,'link_length_km']
        # return the dataframe
        return df3
    # under default condition, return total flow dataframe
    else:
        return df2

def fix_incorrect_coord(df):
    '''
    This function is used to fix coordinates of count points to make it easier to mark them on map

    input
        df (dataframe): any dataframe generated from "dft_rawcount_region_id_3.csv"
    output
        df (dataframe): dataframe with fixed value of coordinate of count points
    '''
    # set the two columns 'latitude' and 'longitude' as string type
    df = df.astype({'latitude':'str', 'longitude':'str'})
    # set values end up with '.0' as zero
    df[df['latitude'].str.endswith('.0')] = '0'
    df[df['longitude'].str.endswith('.0')] = '0'
    # set all values as float type
    df = df.astype({'latitude':'float', 'longitude':'float'})
    # figure out maximum value of latitude and longitude of each count point
    df['latitude'] = df.groupby('count_point_id')['latitude'].transform('max')
    df['longitude'] = df.groupby('count_point_id')['longitude'].transform('max')
    # return the dataframe
    return df
