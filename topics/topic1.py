# This script is aimed to record codes for topic 1 in the report
# and input df is refered to "dft_rawcount_region_id_3.csv" by default

from functions import dealing_data
import numpy as np
import pandas as pd

def topic1_1(df):
    '''
    This function is used to find out number of roads in each authority that mentioned in the given dataframe

    input
        df (dataframe): any given dataframe generated from "dft_rawcount_region_id_3.csv"
    output
        df_num_of_road (dataframe): dataframe of numbers of roads in each authority
    '''
    # dataframe df_r_name is used to include all road names except for those of U/C type
    # dataframe df_no_r_name is used to include all U/C roads
    df_r_name, df_no_r_name = dealing_data.split_by_road(df)
    # group by 'local_authority_name' and count number of roads in each authority
    df_num_of_road = df_r_name.groupby('local_authority_name')['road_name'].nunique()

    return df_num_of_road

def topic1_2(df):
    '''
    This function is used to work out a list of dataframes for each city with the road name
    which have max traffic flow, and the value of that flow

    input
        df (dataframe): any given dataframe generated from "dft_rawcount_region_id_3.csv"
    output
        return_list (list): list of dataframes for each city with the road name which have max traffic flow,
        and the value of that flow
    '''
    def city_max_flow(df1):
        '''
        This function is used to return a dataframe for a specific city with the road name which have
        max traffic flow, and the value of that flow

        input
            df1 (dataframe): given dataframe of some city generated from "dft_rawcount_region_id_3.csv"
        output
            df_return (dataframe): dataframe for this city with the road name which have max traffic flow,
            and the value of that flow
        '''
        # only major and minor roads will be considered so can split the dataframe into two parts
        df_M, df_m = dealing_data.split_by_road(df1, split_by = 'Mm')
        # figure out total flow of each road in df_M and set values of total flow as float type
        df_M = dealing_data.add_col(df_M)
        df_M = df_M.astype({'total_flow': 'float'})
        # group by 'local_authority_name', 'road_name' and work out mean values of total flow for each road
        df_Mcopy = df_M.copy()
        flow_each_road = df_Mcopy.groupby(['local_authority_name', 'road_name'])['total_flow'].mean() # series
        # reset flow_each_road as dataframe
        flow_each_road = flow_each_road.reset_index()
        # find out index of the max term in flow_each_road
        max_road_index = flow_each_road.groupby('local_authority_name')['total_flow'].idxmax()
        # group by authority and work out the max total flow
        max_flow_each_city = flow_each_road.groupby('local_authority_name')['total_flow'].max()

        # create an empty list to store road's name with max flow
        max_road_name = []
        for i in max_road_index:
            max_road_name.append(flow_each_road.loc[i,'road_name'])
        # reset max_road_name as a series
        max_road_name = pd.Series(max_road_name, index = max_flow_each_city.index)
        # put road name series and corresponding max flow series in dataframe df_return and rename the column
        df_return = pd.concat([max_road_name, max_flow_each_city],axis = 1)
        df_return = df_return.rename(columns = {0:'road_name'})
        # sort 'total_flow'
        df_return = df_return.sort_values(by='total_flow', ascending=False)

        return df_return

    # create list for years between 2000.01.01-2010.01.01 and 2010.01.01-2020.01.01
    slot_list1 = ['200{}-01-01'.format(i) for i in range(10)]
    slot_list2 = ['20{}-01-01'.format(i) for i in range(10,21)]
    slot_list = slot_list1 + slot_list2
    return_list = []
    # create return_list to store each year's max flow information from function city_max_flow
    for i in range(len(slot_list) - 1):
            return_list.append(city_max_flow(dealing_data.pick_by_day(slot_list[i],slot_list[i+1],df)))
    return return_list

def topic1_3(df):
    '''
    This function is specificly used for topic 1_3 to work out a number of check points for each city

    input
        df (dataframe): any given dataframe generated from "dft_rawcount_region_id_3.csv"
    output
        merge_df (dataframe): dataframe of number of check points for each city with corresponding indices
    '''
    def get_num_of_cp(df1):
        '''
        This sunction is used to get a series of number of check points for each authority

        input
            df1 (dataframe): any given dataframe generated from "dft_rawcount_region_id_3.csv"
        output
            df_num_of_cp (dataframe): dataframe for each city with the number of check points
        '''
        # dataframe df2 is a subset of df1 with two columns
        df2 = df1[['local_authority_name','count_point_id']]
        # group by authority, count number of check points
        df_num_of_cp = df2.groupby('local_authority_name')['count_point_id'].nunique()
        # return the dataframe
        return df_num_of_cp

    # create list for years between 2000.01.01-2010.01.01 and 2010.01.01-2020.01.01
    slot_list1 = ['200{}-01-01'.format(i) for i in range(10)]
    slot_list2 = ['20{}-01-01'.format(i) for i in range(10,21)]
    slot_list = slot_list1 + slot_list2
    # use list idx_name to store indices of elements in get_num_of_cp
    idx_name = get_num_of_cp(df).index.values.tolist()
    # then use dataframe merge_df to store the indices(this helps avoiding mistake in .join process)
    merge_df = pd.DataFrame(index = idx_name)

    # for each year, newdf includes number of check points of each city,
    # and then join them with their indices
    for i in range(len(slot_list) - 1):
        newdf = get_num_of_cp(dealing_data.pick_by_day(slot_list[i],slot_list[i+1],df))
        newdf = newdf.rename('num_col{}'.format(i+1))
        merge_df = merge_df.join(newdf)
    return merge_df

def topic1_4_1(df):
    '''
    This function is designed for topic 1_4 to get a series of num of authority of each named road across(sorted)

    input
        df (dataframe): any given dataframe generated from "dft_rawcount_region_id_3.csv"
    output
        return_series (series): series of number of cities that each named road across
    '''
    # split given dataframe into df(including all named roads) and dfuseless(including U/C roads)
    df, dfuseless = dealing_data.split_by_road(df)
    # group by road_name and count number of cities sharing the same road
    return_series = df.groupby('road_name')['local_authority_name'].nunique().sort_values(ascending=False)
    return return_series

def topic1_4_2(df):
    '''
    This function is designed to get a series of total length of each named road.(sorted)

    input
        df (dataframe): any given dataframe generated from "dft_rawcount_region_id_3.csv"
    output
        df (dataframe): dataframe with each named road's length
    '''
    # split given dataframe into df(including all Major roads) and dfuseless(including all Minor roads)
    df, dfuseless = dealing_data.split_by_road(df, split_by = 'Mm')
    # delete all duplicates of the same road
    df = df.drop_duplicates(subset=['count_point_id', 'local_authority_name'])
    # return the updated dataframe after sorting values
    return df.groupby('road_name')['link_length_km'].sum().sort_values(ascending=False)


def topic1_4_3(df):
    '''
    This function is used to return a series of num_of CP for each named road

    input
        df (dataframe): any given dataframe generated from "dft_rawcount_region_id_3.csv"
    output
        df (dataframe): dataframe with each named road's number of CPs
    '''
    # split given dataframe into df(including all named roads) and dfuseless(including all U/C roads)
    df, dfuseless = dealing_data.split_by_road(df, split_by = 'UC')
    # return the updated dataframe after sorting values
    return df.groupby('road_name')['count_point_id'].nunique().sort_values(ascending=False)


def topic1_5_1(df):
    '''
    This function is designed to return a dataframe of mean total flow of each named road

    input
        df (dataframe): any given dataframe generated from "dft_rawcount_region_id_3.csv"
    output
        merge_df (dataframe): dataframe with each named road's mean total flow
    '''
    def get_traffic_each_road_type(df1):
        '''
        This function is used to get a series of traffic flow per hour for each type of road

        input
            df1 (dataframe): any given dataframe generated from "dft_rawcount_region_id_3.csv"
        '''
        # add total flow column in given dataframe
        df1 = dealing_data.add_col(df1)
        # work out subset of given dataframe for each type of road
        df_A = df1.loc[df1['road_name'].str.contains('A', case=False)]
        df_B = df1.loc[df1['road_name'].str.contains('B', case=False)]
        df_M = df1.loc[df1['road_name'].str.contains('M', case=False)]
        df_UC1 = df1[df1['road_name'].isin(['U'])]
        df_UC2 = df1[df1['road_name'].isin(['C'])]
        df_UC = df_UC1.append(df_UC2)
        df_UC = df_UC.reset_index(drop = True)
        # return a series carrying all types of road's total flow
        return pd.Series([df_A['total_flow'].mean(),df_B['total_flow'].mean(),df_M['total_flow'].mean(),
                          df_UC['total_flow'].mean()],index=['Road_A', 'Road_B', 'Road_M', 'Road_UC'])

    # create list for years between 2000.01.01-2010.01.01 and 2010.01.01-2020.01.01
    slot_list1 = ['200{}-01-01'.format(i) for i in range(10)]
    slot_list2 = ['20{}-01-01'.format(i) for i in range(10,21)]
    slot_list = slot_list1 + slot_list2
    # use list idx_name to store indices of elements in get_traffic_each_road_type
    idx_name = get_traffic_each_road_type(df).index.values.tolist()
    # then use dataframe merge_df to store the indices(this helps avoiding mistake in .join process)
    merge_df = pd.DataFrame(index = idx_name)

    # for each year, newdf includes mean traffic flow of each type of road
    # and then join them with their indices
    for i in range(len(slot_list) - 1):
        newdf = get_traffic_each_road_type(dealing_data.pick_by_day(slot_list[i],slot_list[i+1],df))
        newdf = newdf.rename(slot_list[i])
        merge_df = merge_df.join(newdf)
    return merge_df


def topic1_5_2(df):
    '''
    This function is designed to return a dataframe of total flow of each type of vehicle

    input
        df (dataframe): any given dataframe generated from "dft_rawcount_region_id_3.csv"
    output
        merge_df (dataframe): dataframe with total flow of each type of vehicle
    '''
    def get_traffic_each_vehicle_type(df1):
        '''
        This function is used to return a series of total flow for each type of vehicle

        input
            df1 (dataframe): any given dataframe generated from "dft_rawcount_region_id_3.csv"
        '''
        # return_series stores total flow of each type of vehicle
        return_series = pd.Series([df1['pedal_cycles'].sum(),
                        df1['two_wheeled_motor_vehicles'].sum(),
                        df1['cars_and_taxis'].sum(),
                        df1['buses_and_coaches'].sum(),
                        df1['lgvs'].sum(),
                        df1['all_hgvs'].sum()],
                        index=['pedal_cycles',
                            'two_wheeled_motor_vehicles', 'cars_and_taxis',
                             'buses_and_coaches', 'lgvs', 'all_hgvs'])
        # return the series
        return return_series

    # create list for years between 2000.01.01-2010.01.01 and 2010.01.01-2020.01.01
    slot_list1 = ['200{}-01-01'.format(i) for i in range(10)]
    slot_list2 = ['20{}-01-01'.format(i) for i in range(10,21)]
    slot_list = slot_list1 + slot_list2
    # use list idx_name to store indices of elements in get_traffic_each_vehicle_type
    idx_name = get_traffic_each_vehicle_type(df).index.values.tolist()
    # then use dataframe merge_df to store the indices(this helps avoiding mistake in .join process)
    merge_df = pd.DataFrame(index = idx_name)

    # for each year, newdf includes traffic flow of each type of vehicle
    # and then join them with their indices
    for i in range(len(slot_list) - 1):
        newdf = get_traffic_each_vehicle_type(dealing_data.pick_by_day(slot_list[i],slot_list[i+1],df))
        newdf = newdf.rename(slot_list[i])
        merge_df = merge_df.join(newdf)
    return merge_df
