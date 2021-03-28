# This script is designed specificly for topic 3 in report

from functions import dealing_data
import numpy as np
import pandas as pd

def topic3_1_get_CP_coordinates(df):
    '''
    This function is designed to return a list of coordinates of CPs in Edinburgh
    and a series of name of roads which across edinburgh and other cities

    input
        df (dataframe): any given dataframe generated from "dft_rawcount_region_id_3.csv"
    output
        the_list (list): list of coordinates of CPs in Edinburgh
        d['road_name'].values (series): series of name of roads which across edinburgh and other cities
    '''
    # split given dataframe into df(including all Major and Minor roads) and df1(including all U/C roads)
    df,df1 = dealing_data.split_by_road(df)
    # use function fix_incorrect_coord to update gven dataframe by fixing coordinates of CPs
    df = dealing_data.fix_incorrect_coord(df)
    # in this topic, Edinburgh is focused on
    a = dealing_data.pick_by_city('edinburgh', df)
    # delete all duplicates of the same road name
    some_values = a['road_name'].drop_duplicates()
    # select all rows containing road names included in dataframe some_values
    b = df.loc[df['road_name'].isin(some_values)]
    # group by road name and count number of cities
    c = b.groupby('road_name')['local_authority_name'].nunique().reset_index()
    # d['road_name'] : series of name of roads which across edinburgh and other cities
    d = c[c['local_authority_name'] > 1]
    the_list = []
    # every element in the_list is the coords of a road
    for i in d['road_name']:
        the_list.append(a[a['road_name']==i][['latitude','longitude']].drop_duplicates().values)
    # return the coordinates list and series of name of roads which across edinburgh and other cities
    return the_list, d['road_name'].values

def topic3_1_analysis(coor, df):
    '''
    This function is used to return a df with column 1: direction_of_travel, column 2:hour and column 3:cars_and_taxis

    input
        df (dataframe): any given dataframe generated from "dft_rawcount_region_id_3.csv"
    '''
    # split given dataframe into df(including all Major and Minor roads) and df1(including all U/C roads)
    df,df1 = dealing_data.split_by_road(df)
    # fix coordinates in df
    df = dealing_data.fix_incorrect_coord(df)
    # df_that_CP is used to store all rows that has appointed coordinates
    df_that_CP = df[(df['latitude']==coor[0])&(df['longitude']==coor[1])]
    # return the dataframe with averaged flow of cars_and_taxis
    df_that_CP = df_that_CP.astype({'cars_and_taxis':'float'})

    return df_that_CP.groupby(['direction_of_travel', 'hour'])['cars_and_taxis'].mean().reset_index()
