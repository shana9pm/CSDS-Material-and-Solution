import json
import csv
import pandas as pd
import numpy as np
import re
import math

"""
This assignment can be done in groups of 3 students. Everyone must submit individually.

Write down the UNIs of your group (if applicable)

Leaderboard Competition Username: tonghong_chen
Leaderboard Competition Email: tc2894@columbia.edu

Member 1: Haiqi Li
Member 1 Uni: hl3115

Member 2: Tonghong Chen
Member 2 Uni: tc2894

Member 3: Xuewei Du
Member 3 Uni: xd2199

"""
locus = None
foursquare = None
final_match_list =None

def get_matches(locu_train_path, foursquare_train_path, matches_train_path, locu_test_path, foursquare_test_path):
    """
        In this function, You need to design your own algorithm or model to find the matches and generate
        a matches_test.csv in the current folder.

        you are given locu_train, foursquare_train json file path and matches_train.csv path to train
        your model or algorithm.

        Then you should test your model or algorithm with locu_test and foursquare_test json file.
        Make sure that you write the test matches to a file in the same directory called matches_test.csv.

    """
    """### Data Preparation"""


    global locus
    locus = pd.read_json(locu_test_path)
    global foursquare
    foursquare = pd.read_json(foursquare_test_path)
    global final_match_list
    final_match_list = []

    """### General Function

    ##### Utils Functions
    """

    def phone_regex(phone_series):
        """
        phone_regex takes in a series of phone number strings and returns numerical phone numbers
        Args:
            phone_series: pandas series where phone numbers are encoded like string "(212) 281-4136",
            string "2122814136", Nonetype, or empty string
        Returns:
            a pandas series having the same length as input phone_series. Phone numbers are now numeric: 2122814136, or Nonetype
        """
        phone_return = []
        for s in phone_series:
            if isinstance(s, str):
                if len(s) == 14:
                    phone_split_list = re.split("\(|\) |-", s)
                    phone_concat = phone_split_list[1] + phone_split_list[2] + phone_split_list[3]
                    phone_num = int(float(phone_concat))
                elif len(s) == 10:
                    phone_num = int(float(s))
                else:
                    phone_num = None
            else:
                phone_num = None
            phone_return.append(phone_num)
        return pd.Series(phone_return)


    def get_distance(long1, lat1, long2, lat2):
        """
        get_distance function computes the distance between two points given their longitudes and latitudes.
        If any of the four inputs is missing, it will return 999 miles, meaning very far away.

        This function assumes that the two points are at similar latitudes. e.g. both points are in New York.
        It is based on this formula: 1 degree of Longitude = cosine (latitude in decimal
                degrees) * length of degree (miles) at equator
        Citation1: https://gis.stackexchange.com/questions/142326/calculating-longitude-length-in-miles
        Citation2: https://www.thoughtco.com/degree-of-latitude-and-longitude-distance-4070616

        Args:
            long1: longitude in decimal of the first point
            lat1: latitude in decimal of the first point
            long2: longitude in decimal of the second point
            lat2: latitude in decimal of the second point
        Returns:
            distance in miles(numerical). If missing any input, will return 999.
        """
        if any([long1 is None, lat1 is None, long2 is None, lat2 is None]) or any(
          [np.isnan(lat1), np.isnan(long1), np.isnan(long2), np.isnan(lat2)]):
            return 999
        miles_per_degree_long = math.cos(math.radians(np.mean([lat1, lat2]))) * 69.172
        miles_per_degree_lat = 69
        miles_dist = math.sqrt(
            (math.fabs(long1 - long2) * miles_per_degree_long) ** 2 + (math.fabs(lat1 - lat2) * miles_per_degree_lat) ** 2)
        return miles_dist


    def iterative_levenshtein(s, t):
        """
            iterative_levenshtein(s, t) -> ldist
            ldist is the Levenshtein distance between the strings
            s and t.
            For all i and j, dist[i,j] will contain the Levenshtein
            distance between the first i characters of s and the
            first j characters of t
        """
        rows = len(s) + 1
        cols = len(t) + 1
        dist = [[0 for x in range(cols)] for x in range(rows)]
        # source prefixes can be transformed into empty strings
        # by deletions:
        for i in range(1, rows):
            dist[i][0] = i
        # target prefixes can be created from an empty source string
        # by inserting the characters
        for i in range(1, cols):
            dist[0][i] = i

        for col in range(1, cols):
            for row in range(1, rows):
                if s[row - 1] == t[col - 1]:
                    cost = 0
                else:
                    cost = 1
                dist[row][col] = min(dist[row - 1][col] + 1,  # deletion
                                     dist[row][col - 1] + 1,  # insertion
                                     dist[row - 1][col - 1] + cost)  # substitution
        return dist[row][col]


    def extract_web_name(web):
        match = re.search('^http.*\.([a-zA-Z0-9]*)\.', web)
        if match is None:
            match = re.search('^http.*\/([a-zA-Z0-9_\-]*)\.', web)

        if match is None:
            print("the error web is. " + web)
        return match.group(1)


    def index_to_id(index, data):
        return data.iloc[index].id


    def delete_match(matched_list):
        """
        Remove the entities that have been matched.

        Args:
        matched list:        list of matched id
            example:[(<locus_id>,<foursquare_id>)]
            locu,foursquare:original data list

        Returns:
            null

        Notice:this function change global variable
        """
        global locus
        global foursquare
        global final_match_list

        final_match_list += matched_list
        for i, j in matched_list:
            locus = locus[locus.id != i]
            foursquare = foursquare[foursquare.id != j]
        return


    def count_accuracy(pair_result, match_table=pd.read_csv("matches_train.csv")):
        counter = 0
        true_count = 0
        for key, value in pair_result:
            counter += 1
            true_value = match_table.loc[match_table.locu_id == key].iloc[0, 1]
            if value == true_value:
                true_count += 1

        return 1.0 * true_count / counter


    """##### SIngle Filter Functions"""


    def match_phone(locus_phone_series, foursquare_phone_series, locus, foursquare):
        """
            match phone numbers
            a filter
        Args:
           locus_phone_series:output of phone_regex(locus.phone)
           foursquare_phone_series:output of phone_regex(foursquare.phone)
           foursquare_phone_series,locus,foursquare:original dataframe.
        Returns:
            list of matched id
            example:[(<locus_id>,<foursquare_id>)]
        """
        cache = []
        for i in range(len(locus_phone_series)):

            i_item = locus_phone_series[i]
            if i_item is not None:
                if len(str(i_item)) > 0:

                    for j in range(len(foursquare_phone_series)):
                        j_item = foursquare_phone_series[j]
                        if j_item is not None:
                            if len(str(j_item)) > 0:

                                if i_item == j_item:
                                    match_element = (index_to_id(i, locus), index_to_id(j, foursquare))
                                    cache.append(match_element)
        return cache


    def match_long_lat_distance(locus, foursquare, bias=0):
        """
            match the distance with
            a filter
        """
        cache = []
        for i in range(locus.shape[0]):
            i_longitude = locus.iloc[i].longitude
            i_latitude = locus.iloc[i].latitude

            if i_longitude is not None and i_latitude is not None:
                if len(str(i_longitude)) > 0 and len(str(i_latitude)) > 0:

                    for j in range(foursquare.shape[0]):
                        j_longitude = foursquare.iloc[j].longitude
                        j_latitude = foursquare.iloc[j].latitude

                        if j_longitude is not None and j_latitude is not None:
                            if len(str(j_longitude)) > 0 and len(str(j_latitude)) > 0:

                                new_dist = get_distance(float(i_longitude), float(i_latitude),
                                                        float(j_longitude), float(j_latitude))
                                if new_dist <= bias:
                                    match_element = (index_to_id(i, locus), index_to_id(j, foursquare))
                                    cache.append(match_element)

        return cache


    def match_street(locus, foursquare, bias=0):
        """
            match the distance with
            a filter
        """
        cache = []
        for i in range(locus.shape[0]):
            i_street_address = locus.iloc[i].street_address
            if i_street_address is not None:
                if len(str(i_street_address)) > 0:

                    for j in range(foursquare.shape[0]):
                        j_street_address = foursquare.iloc[j].street_address
                        if j_street_address is not None:
                            if len(str(j_street_address)) > 0:

                                levenshtein_dist = iterative_levenshtein(str(i_street_address),
                                                                         str(j_street_address))

                                if levenshtein_dist <= bias:
                                    match_element = (index_to_id(i, locus), index_to_id(j, foursquare))
                                    cache.append(match_element)

        return cache


    def group_match_name(locus, foursquare, bias=0):
        """
        This is a very special filter, which can either for filtering or grouping.
        If there is only one pair restaurant name result in the two datsets, this function serves like a filter.
        If there are more than one pair restaurant name results in the two dataset, this function serves like a grouper.
        """
        group = {}
        for i in range(locus.shape[0]):

            i_name = locus.iloc[i]['name']
            if i_name is not None:
                if len(str(i_name)) != 0:

                    for j in range(foursquare.shape[0]):
                        j_name = foursquare.iloc[j]['name']

                        if j_name is not None:
                            if len(str(j_name)) != 0:

                                temp_distance = iterative_levenshtein(
                                    re.sub('[\(\)\.!@#$\' ]', '', str(i_name).lower().strip()),
                                    re.sub('[\(\)\.!@#$\' ]', '', str(j_name).lower().strip()))
                                if temp_distance <= bias:
                                    key = index_to_id(i, locus)
                                    value = index_to_id(j, foursquare)
                                    if key in group:
                                        group[key].append(value)
                                    else:
                                        group[key] = [value]

        # use the group_name function as a filter
        cache = []
        delete_list = []
        for key, value in group.items():
            if len(value) == 1:  # only one match
                cache.append((key, value[0]))
                delete_list.append(key)
        for delete_key in delete_list:
            del group[delete_key]

        return (group, cache)


    """### Test Function"""





    """### 1st filter layer with phone"""

    foursquare_phone = phone_regex(foursquare.phone)
    locus_phone = phone_regex(locus.phone)
    matched_by_phone = match_phone(locus_phone, foursquare_phone, locus, foursquare)

    delete_match(matched_by_phone)

    """### 2nd filter layer with longitude/latitude"""

    matched_by_long_lat_distance = match_long_lat_distance(locus, foursquare)

    delete_match(matched_by_long_lat_distance)

    """### 3rd filter layer with street address"""

    matched_by_street = match_street(locus, foursquare)

    delete_match(matched_by_street)

    """### 4th filter layer with restaurant name"""

    name_group, single_match_by_name = group_match_name(locus, foursquare, bias=0)

    delete_match(single_match_by_name)

    """### 5th filter layer with grouped restaurant name"""

    distances = []
    for key, values in name_group.items():
        i_rest = locus.loc[locus.id == key]
        i_latitude = float(i_rest.latitude)
        i_longitude = float(i_rest.longitude)

        distance = []
        for j in range(len(values)):
            j_rest = foursquare.loc[foursquare.id == values[j]]
            j_latitude = float(j_rest.latitude)
            j_longitude = float(j_rest.longitude)

            temp_distance = get_distance(i_longitude, i_latitude, j_longitude, j_latitude)
            distance.append((temp_distance, j, key, values[j]))

        distance.sort(key=lambda k: k[0])
        distances.append(distance)

    match_by_name_group = []
    for match_results in distances:
        best_match = match_results[0]
        item1 = locus.loc[locus.id == best_match[2]]
        item2 = foursquare.loc[foursquare.id == best_match[3]]

        item1_zipcode = item1.postal_code.to_string(index=False)
        item2_zipcode = item2.postal_code.to_string(index=False)

        if best_match[0] <= 0.02:  # extremely close
            if item1_zipcode is not None and item2_zipcode is not None:
                if len(item1_zipcode) > 0 and len(item2_zipcode) > 0:
                    if int(item1_zipcode) == int(item2_zipcode):
                        match_by_name_group.append([best_match[2], best_match[3]])

    delete_match(match_by_name_group)

    """### Output"""

    pd_outcome = pd.DataFrame(final_match_list, columns=['locu_id', 'foursquare_id'])

    pd_outcome.to_csv('matches_test.csv', index=False)

get_matches("locu_train.json","foursquare_train.json","matched_train.csv","locu_test.json","foursquare_test.json")
