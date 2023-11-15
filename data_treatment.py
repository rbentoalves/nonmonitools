import pandas as pd
from datetime import datetime
import re
import PySimpleGUI as sg
import math
import perfonitor.inputs as inputs
import numpy as np
import os
import openpyxl
import sys



# <editor-fold desc="Dataframe completion">

def get_all_units_from_operation_hours(df_operation_hours):
    inverters = df_operation_hours.columns.drop('Timestamp')
    inverter_operation = {}

    for inverter in inverters:
        # print(inverter)
        df_inverter = df_operation_hours[['Timestamp', inverter]].dropna().reset_index(None, drop=True)
        df_inverter_first_hour = df_inverter.loc[df_inverter[inverter] == 1]['Timestamp'][0]
        df_inverter_last_hour = df_inverter['Timestamp'][len(df_inverter) - 1]

        list_1 = list(df_inverter[inverter])
        list_2 = list(df_inverter[inverter])
        list_1.pop(0)
        list_2.pop(len(list_2) - 1)

        df_inverter['Diff'] = list(np.subtract(np.array(list_1), np.array(list_2))).insert(len(list_2), 0)
        df_change = df_inverter.loc[df_inverter['Diff'] < 0]
        first_hour_timestamp = df_inverter.loc[df_inverter[inverter] == 1]

        if len(df_change) == 0:
            inverter_operation[inverter] = [df_inverter_first_hour, df_inverter_last_hour]

        elif len(df_change) == 1:
            inverter_operation[inverter] = [df_inverter_first_hour, list(df_change['Timestamp'])[0]]

            inverter_name = inverter + ".r" + str(2)
            inverter_operation[inverter_name] = [list(df_change['Timestamp'])[0], df_inverter_last_hour]

        else:
            n_changes = len(df_change)
            print("Found " + str(n_changes) + " changes on ", inverter)

            for i in range(n_changes):
                # print(i)
                if i == 0:
                    inverter_operation[inverter] = [df_inverter_first_hour, list(df_change['Timestamp'])[i]]

                elif i == n_changes:
                    inverter_name = inverter + ".r" + str(i + 1)
                    inverter_operation[inverter_name] = [list(df_change['Timestamp'])[i], df_inverter_last_hour]
                else:
                    inverter_name = inverter + ".r" + str(i + 1)
                    inverter_operation[inverter_name] = [list(df_change['Timestamp'])[i],
                                                         list(df_change['Timestamp'])[i + 1]]

    return inverter_operation

def complete_dataset_inverterops_data(incidents_site, inverter_operation, df_operation_hours):
    for index, row in incidents_site.iterrows():
        component = row['Related Component']
        incident_time = row['Event Start Time']
        # print(component, incident_time)

        # Type of component
        if "Block" in component:
            # print('here')
            incidents_site.loc[index, 'Component Type'] = "Inverter Block"
            incidents_site.loc[index, 'Unit Component'] = "N/A"
            incidents_site.loc[index, 'Operation Time'] = "N/A"

        elif "LSBP" in component:
            incidents_site.loc[index, 'Component Type'] = "Site"
            incidents_site.loc[index, 'Unit Component'] = "N/A"
            incidents_site.loc[index, 'Operation Time'] = "N/A"

        elif "CB" in component or "String" in component:
            incidents_site.loc[index, 'Component Type'] = "Combiner Box"
            incidents_site.loc[index, 'Unit Component'] = "N/A"
            incidents_site.loc[index, 'Operation Time'] = "N/A"

        else:
            incidents_site.loc[index, 'Component Type'] = "Inverter"

            component_number = re.search(r'\d.*', component).group()

            inverter_operation_time_info = [inverter for inverter in inverter_operation if
                                            str(component_number) in inverter]

            for unit in inverter_operation_time_info:
                stime = inverter_operation[unit][0]
                etime = inverter_operation[unit][1]

                if incident_time > stime and incident_time < etime:
                    incidents_site.loc[index, 'Unit Component'] = unit
                    rounded_incident_time = incident_time.round('15min', 'shift_backward')

                    # print(incident_time, rounded_incident_time)

                    incident_operation_time = \
                        df_operation_hours.loc[df_operation_hours['Timestamp'] == rounded_incident_time][
                            component].values[
                            0]

                    changed = False
                    while np.isnan(incident_operation_time):
                        rounded_incident_time = rounded_incident_time - pd.Timedelta(minutes=15)
                        incident_operation_time = \
                            df_operation_hours.loc[df_operation_hours['Timestamp'] == rounded_incident_time][
                                component].values[0]
                        changed = True
                    if changed == True:
                        print("Changed rounded time to forward timestamp because backward was NaN, new timestamp: ",
                              rounded_incident_time)

                    incidents_site.loc[index, 'Operation Time'] = float(incident_operation_time)
                else:
                    continue

    return incidents_site

def timeframe_of_analysis_with_opshours(df_operation_hours):
    start_date_data = df_operation_hours['Timestamp'][0].date()
    end_date_data = df_operation_hours['Timestamp'][len(df_operation_hours) - 1].date()

    sg.theme('DarkAmber')  # Add a touch of color
    # All the stuff inside your window.

    layout = [[sg.Text('Choose number of points:', pad=((2, 10), (2, 5)))],
              [sg.Radio('10', group_id="datapoints", default=False, key="-10DP-"),
               sg.Radio('100', group_id="datapoints", default=True, key="-100DP-"),
               sg.Radio('500', group_id="datapoints", default=False, key="-500DP-")],
              [sg.Text('Enter date of start of analysis', pad=((2, 10), (2, 5)))],
              [sg.CalendarButton('Choose start date', target='-SCAL-',
                                 default_date_m_d_y=(start_date_data.month, start_date_data.day, start_date_data.year),
                                 format="%Y-%m-%d"),
               sg.In(default_text=str(start_date_data), key='-SCAL-', text_color='black', size=(16, 1),
                     enable_events=True, readonly=True, visible=True)],
              [sg.CalendarButton('Choose end date', target='-ECAL-',
                                 default_date_m_d_y=(end_date_data.month, end_date_data.day, end_date_data.year),
                                 format="%Y-%m-%d"),
               sg.In(default_text=str(end_date_data), key='-ECAL-', text_color='black', size=(16, 1),
                     enable_events=True, readonly=True, visible=True)],
              [sg.Button('Submit'), sg.Exit()]]

    # Create the Window
    window = sg.Window('Choose timeframe of analysis', layout)

    # Event Loop to process "events" and get the "values" of the inputs
    while True:
        event, values = window.read(timeout=100)

        if event == sg.WIN_CLOSED or event == 'Exit':  # if user closes window or clicks exit
            window.close()
            return None, None, None

        if event == 'Submit':

            sdate = values['-SCAL-']
            edate = values['-ECAL-']

            if len(sdate) == 0:
                sdate = str(start_date_data)
            # print(values.keys())
            for key in values.keys():
                # print(key)
                if values[key] == True:
                    datapoints = re.search(r'\d+', key).group()
                    # print(datapoints)

            window.close()
            return sdate, edate, datapoints

    # window.close()

    return



# </editor-fold>