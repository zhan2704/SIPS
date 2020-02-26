import dash
import dash_core_components as dcc
import dash_html_components as html
import psycopg2
from config import (user, password, dbname, host, port)
import os
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from geopy.geocoders import Nominatim
from dash.dependencies import Input, Output, State

def ping_postgres_db(db_name):
    '''Tests whether the given PostgreSQL database is live.'''
    try:
        conn = psycopg2.connect(host = host,\
                port=port,\
                user = user, \
                dbname = db_name, \
                password = password, \
                sslmode = "require")
        conn.close()
        return True
    except Exception as e:
        print(e)
        return False

def table_exists(table_name):
    sql_query = "SELECT EXISTS (SELECT 1 FROM "+ table_name + ");"
    try:
        with connection, connection.cursor() as cursor:
            cursor.execute(sql_query)
        return True
    except:
        return False

def get_coordinates(address, timeout=5):
    """
    Geolocate an address.

    Returns the latitude and longitude of the given address using
    OpenStreetMap's Nominatim service. If the coordinates of the
    address cannot be found then ``(None, None)`` is returned.

    As per Nominatim's terms of service this function is rate limited
    to at most one call per second.

    ``timeout`` gives the timeout in seconds.
    """
    geolocator =  Nominatim(user_agent="my-application")
    location = geolocator.geocode(address, timeout=timeout)
    if not location:
        return None, None
    str_lat = str(location.latitude)
    str_long = str(location.longitude) 
    str_loc = str_lat + " " +  str_long
    print(str_loc, type(str_loc))
    return str_loc

print("Testing database...")

connected_to_db = False
while not connected_to_db:
    connected_to_db = ping_postgres_db(dbname)
print("Connected to database successfully!")

conn = psycopg2.connect(host = host, \
		port=port, \
		user = user, \
		dbname = dbname, \
		password = password, \
		sslmode='require')

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

lat_long_str = get_coordinates("2121 7th Ave, Seattle, WA 98121") 

query_from_user = "SELECT * FROM parking_hourly WHERE ST_DWithin(geoloc, ST_GeomFromText" + """(' """\
         + "Point("  + lat_long_str + ")', 4326), 100) AND parking_hourly.date BETWEEN CAST(" \
         + "'2017-01-01' AS date) AND CAST('2017-01-30' AS date)""" \
         + "AND parking_hourly.time BETWEEN 8 AND 10" \
         + "ORDER BY id, date, time ASC"

col_info = "SELECT * FROM collisions WHERE ST_DWithin(geoloc, ST_GeomFromText" + """(' """\
         + "Point("  + lat_long_str + ")', 4326), 500) AND" \
         + "collisions.date BETWEEN CAST('2017-01-01' AS date) AND CAST('2017-01-30' AS date)" \
         + "ORDER BY date, time ASC"

## Initial interface when starting the page:
#read initial dataset and build dfs
parking = pd.read_sql_query(query_from_user, conn)
parking['dot_size'] = parking['total_space'] * 50
park_id = parking['id'].unique()
print(park_id)

collisions = pd.read_sql_query(col_info, conn)
collisions['dot_size'] = collisions['veh_count'] * 120


available_indicators = [6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23 ]

print(collisions.head(3))
print(len(collisions))

# add parking map to the website 
fig = px.scatter_mapbox(parking, lat="lat", lon="long",color_discrete_sequence=["#1e2c63"], \
    size= "dot_size", zoom=15, height=450)
fig.update_layout(mapbox_style="open-street-map")
fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})

# add collision info
fig.add_trace(
    go.Scattermapbox(
        mode= "markers+text", \
        lat=collisions['lat'],\
        lon=collisions['long'],\
        marker=go.scattermapbox.Marker(size=20),\
        text = ['Hit Parked Cars: {}'.format(collisions['hit_parked_car'][i]) for i in range(len(collisions))], \
        hoverinfo = 'text'
    )
)

fig.update_layout(showlegend=False)

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
server = app.server

colors = {'background':'#FCEED9', 'title':'#292826', 'desc':'#5D5C61', 'sub_title':'#3D4B55'}

app.layout = html.Div(style={'backgroundColor': colors['background']}, children=[
    html.H1(
        children='SIPS',
        style={
            'textAlign': 'center',
            'color': colors['title']
        }
    ),

    html.H2(
        children='Safety Information near Parking Surroundings', 
        style={
        'textAlign': 'center',
        'color': colors['sub_title']
        }
    ),

    html.H4(
        children='A platform providing historical collision trends for on-street parking spaces in Seattle near your destination.',
        style={
        'textAlign': 'center',
        'color': colors['desc']
        }
    ),

    #input info
    html.Div([

        html.Div([
            dcc.Markdown('''##### Enter Destination:'''),
            dcc.Input(id='location', placeholder='Address...', type='text', value='2111 7th Ave, Seattle, WA 98121'),

            dcc.Markdown('''##### Enter Start Date:'''),
            dcc.Input(id='start_date', placeholder='Start Date (yyyy-mm-dd)', type='text', value='2017-01-01'),

            dcc.Markdown('''##### Enter End Date:'''),
            dcc.Input(id='end_date', placeholder='End Date (yyyy-mm-dd)', type='text', value='2017-01-15')
        ],
        id='input_dates',
        style={'width': '48%', 'display': 'inline-block'}),


        html.Div([
            dcc.Markdown('''##### Enter Surrounding Distance (m):'''),
            dcc.Input(id='dist', placeholder='100', type='text', value='100'), 

            dcc.Markdown('''##### Choose Start Time:'''),
            dcc.Dropdown(
                id='start_time',
                options=[{'label': i, 'value': i} for i in available_indicators],
                value='8'
            ),

            dcc.Markdown('''##### Choose End Time:'''), 
            dcc.Dropdown( 
                id='end_time',
                options=[{'label': i, 'value': i} for i in available_indicators],
                value='10'
            ) 
        ],
        id='choose_times',
        style={'width': '48%', 'float': 'right', 'display': 'inline-block'})

    ]),

    #submit button
    html.Button('Submit', n_clicks=0, id='button'),
    html.Div(id='button_id'),

    html.H5(id='err', style={'color': 'red'}),
    html.H5(id='output'),

    #input map
    dcc.Graph(
        id='map',
        figure=fig
    ),
    html.Div(id='map_id'),

    #input plot
    dcc.Graph(
        id='historical graph')

])

@app.callback([Output('output', 'children'), 
               Output('err','children'), 
               Output(component_id='map', component_property='figure'), 
               Output('historical graph','figure')],
              [Input('button', 'n_clicks')],
              [State('location', 'value'),
               State('dist', 'value'),
               State('start_date', 'value'),
               State('start_time', 'value'),
               State('end_date', 'value'),
               State('end_time', 'value')])
def update_output(n_clicks, location, start_date, start_time, end_date, end_time):
    if n_clicks is None:
        raise PreventUpdate
    else:
        # change format of user input info
        start_date = start_date.encode('utf-8')
        end_date =end_date.encode('utf-8')
        start_time = str(start_time)
        end_time = str(end_time)
        lat_long_str = get_coordinates(location)
        dist = str(dist)
        # combine the query for parking
        query_from_user = """SELECT * FROM parking_hourly WHERE ST_DWithin(geoloc, ST_GeomFromText('"""\
            + "Point(" \
            + query_from_user1 + lat_long_str + ")'"\
            + start_date + "' AS date) AND CAST('"\
            + end_date + "' AS date) "\
            + "AND parking_hourly.time BETWEEN CAST('"\
            + start_time + "' AS int) AND CAST('"\
            + end_time + "' AS int) ORDER BY id, date, time ASC;"
        # combine the query for collisions
        col_info = "SELECT * FROM collisions WHERE ST_DWithin(geoloc, ST_GeomFromText" + """(' """\
         + "Point("  + lat_long_str + ")', 4326), 500) AND collisions.date BETWEEN CAST('"\
         + start_date + "' AS date) AND CAST('" + end_date + "' AS date)" \
         + "ORDER BY date, time ASC"

        conn = psycopg2.connect(host = host, \
                port=port, \
                user = user, \
                dbname = db_name, \
                password = password, \
                sslmode='require')

        parking = pd.read_sql_query(query_from_user, conn)
        parking['dot_size'] = parking['total_space'] * 50
        collisions = pd.read_sql_query(col_info, conn)

        # show error information if nothing returned from the query
        if len(parking) == 0:
                fig = px.scatter_mapbox(parking, lat="lat", lon="long",color_discrete_sequence=["#1e2c63"], \
                    size= "dot_size", zoom=15, height=600, hover_name="id", hover_data=["total_space"])
                fig.update_layout(mapbox_style="open-street-map")
                fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})

                fig.add_trace(
                    go.Scattermapbox(
                        mode= "markers+text", \
                        lat=collisions['lat'],\
                        lon=collisions['long'],\
                        marker=go.scattermapbox.Marker(size=20),\
                        text = ['Hit Parked Cars: {}'.format(collisions['hit_parked_car'][i]) for i in range(len(collisions))],\
                        hoverinfo = 'text'
                    )
                )

                return '', 'You\'ve entered value that no information found from the database!', dash.no_update, dash.no_update
                raise PreventUpdate

        # update the map per submit
        fig = px.scatter_mapbox(parking, \
            lat="lat", \
            lon="long",\
            color_discrete_sequence=["#1e2c63"],\
            size= "dot_size", 
            zoom=15, \
            height=600, \
            hover_name="id", \
            hover_data=["total_space"]
        )

        fig.update_layout(mapbox_style="open-street-map")
        fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})

        fig.add_trace(
            go.Scattermapbox(
                mode= "markers+text", \
                lat=collisions['lat'],\
                lon=collisions['long'],\
                marker=go.scattermapbox.Marker(size=20),\
                text = ['Hit Parked Cars: {}'.format(collisions['hit_parked_car'][i]) for i in range(len(collisions))], \
                hoverinfo = 'text'
            )
        )
        fig.update_layout(showlegend=False)

        park_id = parking['id'].unique()

    return 'Here is the result:', '', fig, {
            'data': [
                dict(
                    x=parking[parking['id'] == park_id[i] ]["date"], 
                    y=parking[parking['id'] == park_id[i]]["occupanc_rate"], 
                    mode='plot',
                    text=parking[parking['id'] == park_id[i]]['id'], 
                    name=park_id[i]
                ) for i in range(len(park_id))
            ],
            'layout': dict(
                xaxis={'title': 'Date'},
                yaxis={'title': 'Parking Occupancy Rate'},
                legend={i : park_id[i]},
                hovermode='closest',
                plot_bgcolor= colors['background'],
                paper_bgcolor= colors['background'],
                font={
                    'color': colors['sub_title']}
            )
        }



if __name__ == '__main__':
    app.run_server(host='0.0.0.0',debug=True)
