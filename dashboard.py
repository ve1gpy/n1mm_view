#!/usr/bin/python
"""
n1mm_view dashboard
This program displays QSO statistics collected by the collector.
"""

import logging
import calendar
import os
import gc
import multiprocessing
import pygame
import sqlite3
import sys
import time
import matplotlib
import re

#  This makes the code analyzer angry, as python standards say to put imports ahead of all executable code.
#  But... it MUST be RIGHT HERE so matplotlib does not try to use the wrong backend.
matplotlib.use('Agg')
import matplotlib.backends.backend_agg as agg
import matplotlib.pyplot as plt
from matplotlib.dates import HourLocator, DateFormatter
from mpl_toolkits.basemap import Basemap
import numpy as np

from n1mm_view_constants import *
from n1mm_view_config import *

__author__ = 'Jeffrey B. Otterson, N1KDO'
__copyright__ = 'Copyright 2016 Jeffrey B. Otterson'
__license__ = 'Simplified BSD'

RED = pygame.Color('#ff0000')
GREEN = pygame.Color('#33cc33')
BLUE = pygame.Color('#3333cc')
BRIGHT_BLUE = pygame.Color('#6666ff')
YELLOW = pygame.Color('#cccc00')
CYAN = pygame.Color('#00cccc')
MAGENTA = pygame.Color('#cc00cc')
ORANGE = pygame.Color('#ff9900')
BLACK = pygame.Color('#000000')
WHITE = pygame.Color('#ffffff')
GRAY = pygame.Color('#cccccc')

# Initialize font support
pygame.font.init()
view_font = pygame.font.Font('VeraMoBd.ttf', 64)
bigger_font = pygame.font.SysFont('VeraMoBd.ttf', 180)
view_font_height = view_font.get_height()

LOGO_IMAGE_INDEX = 0
QSO_COUNTS_TABLE_INDEX = 1
QSO_RATES_TABLE_INDEX = 2
QSO_OPERATORS_PIE_INDEX = 3
QSO_OPERATORS_TABLE_INDEX = 4
QSO_STATIONS_PIE_INDEX = 5
QSO_BANDS_PIE_INDEX = 6
QSO_MODES_PIE_INDEX = 7
QSO_RATE_CHART_IMAGE_INDEX = 8
SECTIONS_WORKED_MAP_INDEX = 9
IMAGE_COUNT = 10

IMAGE_MESSAGE = 1
CRAWL_MESSAGE = 2

IMAGE_FORMAT = 'RGB'
SAVE_PNG = False

logging.basicConfig(format='%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S',
                    level=LOG_LEVEL)
logging.Formatter.converter = time.gmtime

logging.debug("Checking for HTML_DIR")
if 'HTML_DIR' in globals():
    SAVE_PNG = True
    logging.debug("HTML_DIR set to %s - Will create PNG files" % HTML_DIR)
    # Check if the dir given exists and create if necessary
    if not os.path.exists(HTML_DIR):
        logging.error("%s did not exist - creating but check Apache permissions" % HTML_DIR)
        os.makedirs(HTML_DIR)

if POST_FILE_COMMAND is not None and POST_FILE_COMMAND != "":
    postProcessing = True
    logging.debug("POST_FILE_COMMAND will be executed after file creation. Command = ")
    logging.debug(POST_FILE_COMMAND)
else:
    postProcessing = False


def makePNGTitle(title):
    return ''.join([HTML_DIR, '/', re.sub('[^\w\-_]', '_', title), '.png'])


def save_image(image_data, image_size, filename):
    surface = pygame.image.frombuffer(image_data, image_size, IMAGE_FORMAT)
    pygame.image.save(surface, filename)
    pass


def load_data(size, q, base_map, last_qso_timestamp):
    """
    load data from the database tables
    """
    logging.debug('load data')

    qso_operators = []
    qso_stations = []
    qso_band_modes = []
    operator_qso_rates = []
    qsos_per_hour = []
    qsos_by_section = {}

    db = None
    data_updated = False
    last_qso_time = last_qso_timestamp

    try:
        logging.debug('connecting to database')
        db = sqlite3.connect(DATABASE_FILENAME)
        cursor = db.cursor()
        logging.debug('database connected')

        if logging.getLogger().isEnabledFor(logging.DEBUG):
            cursor.execute('SELECT timestamp, callsign, section FROM qso_log')
            for row in cursor:
                logging.debug('QSO: %s\t%s\t%s' % (row[0], row[1], row[2]))


                # get timestamp from the last record in the database
        cursor.execute('SELECT timestamp, callsign, exchange, section, operator.name, band_id \n'
                       'FROM qso_log JOIN operator WHERE operator.id = operator_id \n'
                       'ORDER BY timestamp DESC LIMIT 1')
        last_qso_time = int(time.time()) - 60
        message = ''
        for row in cursor:
            last_qso_time = row[0]
            message = 'Last QSO: %s %s %s on %s by %s at %s' % (
                row[1], row[2], row[3], Bands.BANDS_TITLE[row[5]], row[4],
                datetime.datetime.utcfromtimestamp(row[0]).strftime('%H:%M:%S'))
            logging.debug(message)

        logging.debug('old_timestamp = %d, timestamp = %d', last_qso_timestamp, last_qso_time)
        if last_qso_time != last_qso_timestamp:
            logging.debug('data updated!')
            data_updated = True
            q.put((CRAWL_MESSAGE, 3, message))

            # load qso_operators
            logging.debug('Load QSOs by Operator')
            qso_operators = []
            cursor.execute('SELECT name, COUNT(operator_id) AS qso_count \n'
                           'FROM qso_log JOIN operator ON operator.id = operator_id \n'
                           'GROUP BY operator_id ORDER BY qso_count DESC;')
            for row in cursor:
                qso_operators.append((row[0], row[1]))

            # load qso_stations
            logging.debug('Load QSOs by Station')
            qso_stations = []
            cursor.execute('SELECT name, COUNT(station_id) AS qso_count \n'
                           'FROM qso_log JOIN station ON station.id = station_id GROUP BY station_id;')
            for row in cursor:
                qso_stations.append((row[0], row[1]))

            qso_band_modes = [[0] * 4 for _ in Bands.BANDS_LIST]

            cursor.execute('SELECT COUNT(*), band_id, mode_id FROM qso_log GROUP BY band_id, mode_id;')
            for row in cursor:
                qso_band_modes[row[1]][Modes.MODE_TO_SIMPLE_MODE[row[2]]] = row[0]

            # calculate QSOs per hour rate for all active operators
            # the higher the slice_minutes number is, the better the
            # resolution of the rate, but the slower to update.
            slice_minutes = 10
            slices_per_hour = 60 / slice_minutes

            # get timestamp from the first record in the database
            logging.debug('Loading first and last QSO timestamps')
            cursor.execute('SELECT timestamp FROM qso_log ORDER BY timestamp LIMIT 1')
            first_qso_time = int(time.time()) - 60
            for row in cursor:
                first_qso_time = row[0]

            start_time = last_qso_time - slice_minutes * 60

            # load QSOs per Hour by Operator
            logging.debug('Load QSOs per Hour by Operator')
            cursor.execute('SELECT operator.name, COUNT(operator_id) qso_count FROM qso_log\n'
                           'JOIN operator ON operator.id = operator_id\n'
                           'WHERE timestamp >= ? AND timestamp <= ?\n'
                           'GROUP BY operator_id ORDER BY qso_count DESC LIMIT 10;', (start_time, last_qso_time))
            operator_qso_rates = [['Operator', 'Rate']]
            total = 0
            for row in cursor:
                rate = row[1] * slices_per_hour
                total += rate
                operator_qso_rates.append([row[0], '%4d' % rate])
            operator_qso_rates.append(['Total', '%4d' % total])

            qsos_per_hour = []
            qsos_by_band = [0] * Bands.count()
            slice_minutes = 15
            slices_per_hour = 60 / slice_minutes
            window_seconds = slice_minutes * 60

            # load QSO rates per Hour by Band
            logging.debug('Load QSOs per Hour by Band')
            cursor.execute('SELECT timestamp / %d * %d AS ts, band_id, COUNT(*) AS qso_count \n'
                           'FROM qso_log GROUP BY ts, band_id;' % (window_seconds, window_seconds))
            for row in cursor:
                if len(qsos_per_hour) == 0:
                    qsos_per_hour.append([0] * Bands.count())
                    qsos_per_hour[-1][0] = row[0]
                while qsos_per_hour[-1][0] != row[0]:
                    ts = qsos_per_hour[-1][0] + window_seconds
                    qsos_per_hour.append([0] * Bands.count())
                    qsos_per_hour[-1][0] = ts
                qsos_per_hour[-1][row[1]] = row[2] * slices_per_hour
                qsos_by_band[row[1]] += row[2]

            for rec in qsos_per_hour:  # FIXME
                rec[0] = datetime.datetime.utcfromtimestamp(rec[0])
                t = rec[0].strftime('%H:%M:%S')

        # load QSOs by Section
        logging.debug('Load QSOs by Section')
        qsos_by_section = {}
        cursor.execute('SELECT section, COUNT(section) AS qsos FROM qso_log GROUP BY section;')
        for row in cursor:
            qsos_by_section[row[0]] = row[1]

        q.put((CRAWL_MESSAGE, 0, ''))

        logging.debug('load data done')
    except sqlite3.OperationalError as error:
        logging.exception(error)
        q.put((CRAWL_MESSAGE, 0, 'database read error', YELLOW, RED))
        return
    finally:
        if db is not None:
            logging.debug('Closing DB')
            cursor.close()
            db.close()
            db = None

    if data_updated:
        try:
            image_data, image_size = qso_summary_table(size, qso_band_modes)
            enqueue_image(q, QSO_COUNTS_TABLE_INDEX, image_data, image_size)
        except Exception as e:
            logging.exception(e)
        try:
            image_data, image_size = qso_rates_table(size, operator_qso_rates)
            enqueue_image(q, QSO_RATES_TABLE_INDEX, image_data, image_size)
        except Exception as e:
            logging.exception(e)
        try:
            image_data, image_size = qso_operators_graph(size, qso_operators)
            enqueue_image(q, QSO_OPERATORS_PIE_INDEX, image_data, image_size)
        except Exception as e:
            logging.exception(e)
        try:
            image_data, image_size = qso_operators_table(size, qso_operators)
            enqueue_image(q, QSO_OPERATORS_TABLE_INDEX, image_data, image_size)
        except Exception as e:
            logging.exception(e)
        try:
            image_data, image_size = qso_stations_graph(size, qso_stations)
            enqueue_image(q, QSO_STATIONS_PIE_INDEX, image_data, image_size)
        except Exception as e:
            logging.exception(e)
        try:
            image_data, image_size = qso_bands_graph(size, qso_band_modes)
            enqueue_image(q, QSO_BANDS_PIE_INDEX, image_data, image_size)
        except Exception as e:
            logging.exception(e)
        try:
            image_data, image_size = qso_modes_graph(size, qso_band_modes)
            enqueue_image(q, QSO_MODES_PIE_INDEX, image_data, image_size)
        except Exception as e:
            logging.exception(e)
        try:
            image_data, image_size = qso_rates_chart(size, qsos_per_hour)
            enqueue_image(q, QSO_RATE_CHART_IMAGE_INDEX, image_data, image_size)
        except Exception as e:
            logging.exception(e)

        # There is a memory leak in the next code
    try:
        image_data, image_size = draw_map(size, qsos_by_section, base_map)
        enqueue_image(q, SECTIONS_WORKED_MAP_INDEX, image_data, image_size)
    except Exception as e:
        logging.exception(e)

    if data_updated:
        if postProcessing:
            os.system(POST_FILE_COMMAND)

    return last_qso_time


def enqueue_image(q, id, image_data, size):
    if not HTML_ONLY:
        if image_data is not None:
            q.put((IMAGE_MESSAGE, id, image_data, size))


def init_display():
    """
    set up the pygame display, full screen
    """

    # Check which frame buffer drivers are available
    # Start with fbcon since directfb hangs with composite output
    drivers = ['fbcon', 'directfb', 'svgalib', 'directx', 'windib']
    found = False
    for driver in drivers:
        # Make sure that SDL_VIDEODRIVER is set
        if not os.getenv('SDL_VIDEODRIVER'):
            os.putenv('SDL_VIDEODRIVER', driver)
        try:
            pygame.display.init()
        except pygame.error:
            #  logging.warn('Driver: %s failed.' % driver)
            continue
        found = True
        logging.debug('using %s driver', driver)
        break

    if not found:
        raise Exception('No suitable video driver found!')

    size = (pygame.display.Info().current_w, pygame.display.Info().current_h)
    pygame.mouse.set_visible(0)
    if driver != 'directx':  # debugging hack runs in a window on Windows
        screen = pygame.display.set_mode(size, pygame.FULLSCREEN)
    else:
        logging.info('running in windowed mode')
        # set window origin for windowed usage
        os.putenv('SDL_VIDEO_WINDOW_POS', '0,0')
        # size = (size[0]-10, size[1] - 30)
        screen = pygame.display.set_mode(size, pygame.NOFRAME)

    logging.debug('display size: %d x %d', size[0], size[1])
    # Clear the screen to start
    screen.fill(BLACK)
    return screen, size


def create_map():
    """
    create the base map for the choropleth.
    """
    logging.debug('create_map() -- Please wait while I create the world.')
    degrees_width = 118.0
    degrees_height = 55.0
    center_lat = 44.5
    center_lon = -110.0
    my_map = Basemap(  # ax=ax,
        projection='merc',  # default is cyl
        ellps='WGS84',
        lat_0=center_lat, lon_0=center_lon,
        llcrnrlat=center_lat - degrees_height / 2.0,
        llcrnrlon=center_lon - degrees_width / 2.0,
        urcrnrlat=center_lat + degrees_height / 2.0,
        urcrnrlon=center_lon + degrees_width / 2.0,
        resolution='i',  # 'c', 'l', 'i', 'h', 'f'
    )
    logging.debug('created map')
    logging.debug('loading shapes...')
    for section_name in CONTEST_SECTIONS.keys():
        # logging.debug('trying to load shape for %s', section_name)
        try:
            my_map.readshapefile('shapes/%s' % section_name, section_name, drawbounds=False)
        except IOError, err:
            logging.error('Could not load shape for %s' % section_name)

    logging.debug('loaded section shapes')
    return my_map


def draw_map(size, qsos_by_section, my_map):
    logging.debug('draw_section map()')
    width_inches = size[0] / 100.0
    height_inches = size[1] / 100.0
    fig = plt.Figure(figsize=(width_inches, height_inches), dpi=100, tight_layout={'pad': 0.10}, facecolor='black')
    water = '#191970'  # '#15155e'
    earth = '#552205'
    if matplotlib.__version__[0] == '1':
        ax = fig.add_subplot(111, axis_bgcolor=water)
    else:
        ax = fig.add_subplot(111, facecolor=water)
    ax.annotate('Sections Worked', xy=(0.5, 1), xycoords='axes fraction', ha='center', va='top',
                color='white', size=48, weight='bold')

    logging.debug('setting basemap axis')
    my_map.ax = ax
    # my_map.drawcoastlines(color='white', linewidth=0.5)
    # my_map.drawcountries(color='white', linewidth=0.5)
    # my_map.drawstates(color='white')
    # my_map.drawmapboundary(fill_color='#000033')
    my_map.fillcontinents(color=earth, lake_color=water)

    # mark our QTH
    x, y = my_map(QTH_LONGITUDE, QTH_LATITUDE)
    my_map.plot(x, y, '.', color='r')
    my_map.nightshade(datetime.datetime.utcnow(), alpha=0.25, zorder=4)

    logging.debug('setting shapes')
    ranges = [0, 1, 10, 20, 50, 100, 200]  # , 500]  # , 1000]
    num_colors = len(ranges)
    # color_palette = ['#223333', '#1c8e66', '#389c66', '#55aa66', '#71b866', '#8ec766', '#aad566', '#c7e366', '#e3f166']
    color_palette = matplotlib.cm.viridis(np.linspace(0.33, 1, num_colors + 1))

    legend_patches = []
    last_bin = 0
    for i in range(0, num_colors):
        bin_max = ranges[i]
        color = color_palette[i]
        if bin_max == 0:
            label = '0'
            color = 'k'
        elif bin_max == -1:
            label = '%d +' % (last_bin + 1)
        else:
            label = '%d - %d' % (last_bin + 1, bin_max)
            last_bin = bin_max
        legend_patches.append(matplotlib.patches.Patch(color=color, label=label))
    label = '> %d' % last_bin
    color = color_palette[num_colors]
    legend_patches.append(matplotlib.patches.Patch(color=color, label=label))
    legend = ax.legend(handles=legend_patches)
    frame = legend.get_frame()
    frame.set_color((0, 0, 0, 0.75))
    frame.set_edgecolor('w')
    legend.get_title().set_color('w')
    for text in legend.get_texts():
        plt.setp(text, color='w')

    # applying choropleth
    # logging.debug('applying choropleth')
    for section_name in CONTEST_SECTIONS.keys():
        qsos = qsos_by_section.get(section_name)
        if qsos is None:
            qsos = 0
        shape = my_map.__dict__.get(section_name)  # probably bad style
        if shape is not None:
            color_index = 0
            for range_max in ranges:
                if range_max == -1 or qsos <= range_max:
                    break
                color_index += 1
                if color_index == num_colors:
                    break

            section_color = 'k' if color_index == 0 else color_palette[color_index]
            # logging.debug('%s %d %d', section_name, qsos, color_index)

            patches = []
            for ss in shape:
                patches.append(matplotlib.patches.Polygon(np.array(ss), True))
            patch_collection = matplotlib.collections.PatchCollection(patches, edgecolor='w', linewidths=0.1, zorder=2)
            patch_collection.set_facecolor(section_color)
            ax.add_collection(patch_collection)

    canvas = agg.FigureCanvasAgg(fig)
    canvas.draw()
    renderer = canvas.get_renderer()
    raw_data = renderer.tostring_rgb()
    if SAVE_PNG:
        logging.debug('Saving PNG file')
        try:
            fig.savefig(makePNGTitle('sections'))
        except:
            logging.exception("Error writing file %s" % makePNGTitle('sections'))

    fig.clf()
    plt.close(fig)
    gc.collect()
    canvas_size = canvas.get_width_height()
    logging.debug('draw_map() done')
    return raw_data, canvas_size


def make_pie(size, values, labels, title):
    """
    make a pie chart using matplotlib.
    return the chart as a pygame surface
    make the pie chart a square that is as tall as the display.
    """
    logging.debug('make_pie(...,...,%s)', title)
    inches = size[1] / 100.0
    fig = plt.figure(figsize=(inches, inches), dpi=100, tight_layout={'pad': 0.10}, facecolor='k')
    ax = fig.add_subplot(111)
    ax.pie(values, labels=labels, autopct='%1.1f%%', textprops={'color': 'w'}, wedgeprops={'linewidth': 0.25},
           colors=('b', 'g', 'r', 'c', 'm', 'y', '#ff9900', '#00ff00', '#663300'))
    ax.set_title(title, color='white', size=48, weight='bold')

    handles, labels = ax.get_legend_handles_labels()
    legend = ax.legend(handles[0:5], labels[0:5], title='Top %s' % title, loc='lower left')  # best
    frame = legend.get_frame()
    frame.set_color((0, 0, 0, 0.75))
    frame.set_edgecolor('w')
    legend.get_title().set_color('w')
    for text in legend.get_texts():
        plt.setp(text, color='w')

    canvas = agg.FigureCanvasAgg(fig)
    canvas.draw()
    renderer = canvas.get_renderer()
    raw_data = renderer.tostring_rgb()

    if SAVE_PNG:
        logging.debug('Saving PNG as %s' % makePNGTitle(title))
        try:
            fig.savefig(makePNGTitle(title), facecolor=fig.get_facecolor(), edgecolor='none')
        except:
            logging.exception("Error writing file %s" % makePNGTitle(title))
    plt.close(fig)

    canvas_size = canvas.get_width_height()
    logging.debug('make_pie(...,...,%s) done', title)
    return raw_data, canvas_size


def show_graph(screen, size, surf):
    """
    display a surface on the screen.
    """
    logging.debug('show_graph()')
    x_offset = (size[0] - surf.get_width()) / 2
    screen.fill((0, 0, 0))
    screen.blit(surf, (x_offset, 0))
    logging.debug('show_graph() done')


def qso_operators_graph(size, qso_operators):
    """
    create the QSOs by Operators pie chart
    """
    # calculate QSO by Operator
    if qso_operators is None or len(qso_operators) == 0:
        return None, (0, 0)
    labels = []
    values = []
    for d in qso_operators:
        labels.append(d[0])
        values.append(d[1])
    return make_pie(size, values, labels, "QSOs by Operator")


def qso_operators_table(size, qso_operators):
    """
    create the Top 5 QSOs by Operators table
    """
    if len(qso_operators) == 0:
        return None, (0, 0)

    count = 0
    cells = [['Operator', 'QSOs']]
    for d in qso_operators:
        cells.append(['%s' % d[0], '%5d' % d[1]])
        count += 1
        if count >= 5:
            break

    if count == 0:
        return None, (0, 0)
    else:
        return draw_table(size, cells, "Top 5 Operators", bigger_font)


def qso_stations_graph(size, qso_stations):
    """
    create the QSOs by Station pie chart
    """
    if qso_stations is None or len(qso_stations) == 0:
        return None, (0, 0)
    labels = []
    values = []
    # for d in qso_stations:
    for d in sorted(qso_stations, key=lambda count: count[1], reverse=True):
        labels.append(d[0])
        values.append(d[1])
    return make_pie(size, values, labels, "QSOs by Station")


def qso_bands_graph(size, qso_band_modes):
    """
    create the QSOs by Band pie chart
    """
    if qso_band_modes is None or len(qso_band_modes) == 0:
        return None, (0, 0)

    labels = []
    values = []
    band_data = [[band, 0] for band in range(0, Bands.count())]
    total = 0
    for i in range(0, Bands.count()):
        band_data[i][1] = qso_band_modes[i][1] + qso_band_modes[i][2] + qso_band_modes[i][3]
        total += band_data[i][1]

    if total == 0:
        return None, (0, 0)

    for bd in sorted(band_data[1:], key=lambda count: count[1], reverse=True):
        if bd[1] > 0:
            labels.append(Bands.BANDS_TITLE[bd[0]])
            values.append(bd[1])
    return make_pie(size, values, labels, "QSOs by Band")


def qso_modes_graph(size, qso_band_modes):
    """
    create the QSOs by Mode pie chart
    """
    if qso_band_modes is None or len(qso_band_modes) == 0:
        return None, (0, 0)

    labels = []
    values = []
    mode_data = [[mode, 0] for mode in range(0, len(Modes.SIMPLE_MODES_LIST))]
    total = 0
    for i in range(0, Bands.count()):
        for mode_num in range(1, len(Modes.SIMPLE_MODES_LIST)):
            mode_data[mode_num][1] += qso_band_modes[i][mode_num]
            total += qso_band_modes[i][mode_num]

    if total == 0:
        return None, (0, 0)

    for md in sorted(mode_data[1:], key=lambda count: count[1], reverse=True):
        if md[1] > 0:
            labels.append(Modes.SIMPLE_MODES_LIST[md[0]])
            values.append(md[1])
    return make_pie(size, values, labels, "QSOs by Mode")


def qso_summary_table(size, qso_band_modes):
    """
    create the QSO Summary Table
    """
    return draw_table(size, make_score_table(qso_band_modes), "QSOs Summary")


def qso_rates_table(size, operator_qso_rates):
    """
    create the QSO Rates by Operator table
    """
    if operator_qso_rates is None or len(operator_qso_rates) < 3:
        return None, (0, 0)
    else:
        return draw_table(size, operator_qso_rates, "QSO/Hour Rates")


def qso_rates_chart(size, qsos_per_hour):
    """
    make the qsos per hour per band chart
    returns a pygame surface
    """
    title = 'QSOs per Hour by Band'
    qso_counts = [[], [], [], [], [], [], [], [], [], []]

    if qsos_per_hour is None or len(qsos_per_hour) == 0:
        return None, (0, 0)

    data_valid = len(qsos_per_hour) != 0

    for qpm in qsos_per_hour:
        for i in range(0, Bands.count()):
            c = qpm[i]
            cl = qso_counts[i]
            cl.append(c)

    logging.debug('make_plot(...,...,%s)', title)
    width_inches = size[0] / 100.0
    height_inches = size[1] / 100.0
    fig = plt.Figure(figsize=(width_inches, height_inches), dpi=100, tight_layout={'pad': 0.10}, facecolor='black')

    if matplotlib.__version__[0] == '1':
        ax = fig.add_subplot(111, axis_bgcolor='black')
    else:
        ax = fig.add_subplot(111, facecolor='black')

    ax.set_title(title, color='white', size=48, weight='bold')

    st = calendar.timegm(EVENT_START_TIME.timetuple())
    lt = calendar.timegm(qsos_per_hour[-1][0].timetuple())
    if data_valid:
        dates = matplotlib.dates.date2num(qso_counts[0])
        colors = ['r', 'g', 'b', 'c', 'm', 'y', '#ff9900', '#00ff00', '#663300']
        labels = Bands.BANDS_TITLE[1:]
        if lt < st:
            start_date = dates[0]  # matplotlib.dates.date2num(qsos_per_hour[0][0].timetuple())
            end_date = dates[-1]  # matplotlib.dates.date2num(qsos_per_hour[-1][0].timetuple())
        else:
            start_date = matplotlib.dates.date2num(EVENT_START_TIME)
            end_date = matplotlib.dates.date2num(EVENT_END_TIME)
        ax.set_xlim(start_date, end_date)

        ax.stackplot(dates, qso_counts[1], qso_counts[2], qso_counts[3], qso_counts[4], qso_counts[5], qso_counts[6],
                     qso_counts[7], qso_counts[8], qso_counts[9], labels=labels, colors=colors, linewidth=0.2)
        ax.grid(True)
        legend = ax.legend(loc='best', ncol=Bands.count() - 1)
        legend.get_frame().set_color((0, 0, 0, 0))
        legend.get_frame().set_edgecolor('w')
        for text in legend.get_texts():
            plt.setp(text, color='w')
        ax.spines['left'].set_color('w')
        ax.spines['right'].set_color('w')
        ax.spines['top'].set_color('w')
        ax.spines['bottom'].set_color('w')
        ax.tick_params(axis='y', colors='w')
        ax.tick_params(axis='x', colors='w')
        ax.set_ylabel('QSO Rate/Hour', color='w', size='x-large', weight='bold')
        ax.set_xlabel('UTC Hour', color='w', size='x-large', weight='bold')
        hour_locator = HourLocator()
        hour_formatter = DateFormatter('%H')
        ax.xaxis.set_major_locator(hour_locator)
        ax.xaxis.set_major_formatter(hour_formatter)
    canvas = agg.FigureCanvasAgg(fig)
    canvas.draw()
    renderer = canvas.get_renderer()
    raw_data = renderer.tostring_rgb()

    if SAVE_PNG:
        logging.debug('Saving PNG as %s' % makePNGTitle(title))
        try:
            fig.savefig(makePNGTitle(title), facecolor=fig.get_facecolor(), edgecolor='none')
        except:
            logging.exception("Error writing file %s" % makePNGTitle(title))

    plt.close(fig)
    canvas_size = canvas.get_width_height()
    return raw_data, canvas_size


def draw_table(size, cell_text, title, font=None):
    """
    draw a table
    """
    logging.debug('draw_table(...,%s)', title)
    if font is None:
        table_font = view_font
    else:
        table_font = font

    text_y_offset = 4
    text_x_offset = 4
    line_width = 4

    # calculate column widths
    rows = len(cell_text)
    cols = len(cell_text[0])
    col_widths = [0] * cols
    widest = 0
    for row in cell_text:
        col_num = 0
        for col in row:
            text_size = table_font.size(col)
            text_width = text_size[0] + 2 * text_x_offset
            if text_width > col_widths[col_num]:
                col_widths[col_num] = text_width
            if text_width > widest:
                widest = text_width
            col_num += 1

    header_width = table_font.size(title)[0]
    table_width = sum(col_widths) + line_width / 2
    row_height = table_font.get_height()
    height = (rows + 1) * row_height + line_width / 2
    surface_width = table_width
    x_offset = 0
    if header_width > surface_width:
        surface_width = header_width
        x_offset = (header_width - table_width) / 2

    surf = pygame.Surface((surface_width, height))

    surf.fill(BLACK)
    text_color = GRAY
    head_color = WHITE
    grid_color = GRAY

    # draw the title
    text = table_font.render(title, True, head_color)
    textpos = text.get_rect()
    textpos.y = 0
    textpos.centerx = surface_width / 2
    surf.blit(text, textpos)

    starty = row_height
    origin = (x_offset, row_height)

    # draw the grid
    x = x_offset
    y = starty
    for r in range(0, rows + 1):
        sp = (x, y)
        ep = (x + table_width, y)
        pygame.draw.line(surf, grid_color, sp, ep, line_width)
        y += row_height

    x = x_offset
    y = starty
    for cw in col_widths:
        sp = (x, y)
        ep = (x, y + height)
        pygame.draw.line(surf, grid_color, sp, ep, line_width)
        x += cw
    sp = (x, y)
    ep = (x, y + height)
    pygame.draw.line(surf, grid_color, sp, ep, line_width)

    y = starty + text_y_offset
    row_number = 0
    for row in cell_text:
        row_number += 1
        x = origin[0]
        column_number = 0
        for col in row:
            x += col_widths[column_number]
            column_number += 1
            if row_number == 1 or column_number == 1:
                text = table_font.render(col, True, head_color)
            else:
                text = table_font.render(col, True, text_color)
            textpos = text.get_rect()
            textpos.y = y - text_y_offset
            textpos.right = x - text_x_offset
            surf.blit(text, textpos)
        y += row_height
    logging.debug('draw_table(...,%s) done', title)
    size = surf.get_size()
    data = pygame.image.tostring(surf, 'RGB')

    if SAVE_PNG:
        logging.debug('Saving table PNG as %s' % makePNGTitle(title))
        try:
            pygame.image.save(surf, makePNGTitle(title))
        except:
            logging.exception("Error writing file %s" % makePNGTitle(title))

    return data, size


def make_score_table(qso_band_modes):
    """
    create the score table from data
    """
    cell_data = [[0 for m in Modes.SIMPLE_MODES_LIST] for b in Bands.BANDS_TITLE]

    for band_num in range(1, Bands.count()):
        for mode_num in range(1, len(Modes.SIMPLE_MODES_LIST)):
            cell_data[band_num][mode_num] = qso_band_modes[band_num][mode_num]
            cell_data[band_num][0] += qso_band_modes[band_num][mode_num]
            cell_data[0][mode_num] += qso_band_modes[band_num][mode_num]

    total = 0
    for c in cell_data[0][1:]:
        total += c
    cell_data[0][0] = total

    # the totals are in the 0th row and 0th column, move them to last.
    cell_text = [['', '   CW', 'Phone', ' Data', 'Total']]
    band_num = 0
    for row in cell_data[1:]:
        band_num += 1
        row_text = ['%5s' % Bands.BANDS_TITLE[band_num]]

        for col in row[1:]:
            row_text.append('%5d' % col)
        row_text.append('%5d' % row[0])
        cell_text.append(row_text)

    row = cell_data[0]
    row_text = ['Total']
    for col in row[1:]:
        row_text.append('%5d' % col)
    row_text.append('%5d' % row[0])
    cell_text.append(row_text)
    return cell_text


def show_page(screen, size, image):
    """
    show a chart image
    """
    logging.debug("show_page()")
    if image is not None:
        show_graph(screen, size, image)
    logging.debug('show_page() done')


def delta_time_to_string(delta_time):
    """
    return a string that represents delta time
    """
    seconds = delta_time.total_seconds()
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    if days != 0:
        return '%d days, %02d:%02d:%02d' % (days, hours, minutes, seconds)
    else:
        return '%02d:%02d:%02d' % (hours, minutes, seconds)


def update_crawl_message(crawl_messages):
    crawl_messages.set_message(0, EVENT_NAME)
    crawl_messages.set_message_colors(0, BRIGHT_BLUE, BLACK)
    now = datetime.datetime.utcnow()
    crawl_messages.set_message(1, datetime.datetime.strftime(now, '%H:%M:%S'))
    if now < EVENT_START_TIME:
        delta = EVENT_START_TIME - now
        seconds = delta.total_seconds()
        bg = BLUE if seconds > 3600 else RED
        crawl_messages.set_message(2, '%s starts in %s' % (EVENT_NAME, delta_time_to_string(delta)))
        crawl_messages.set_message_colors(2, WHITE, bg)
    elif now < EVENT_END_TIME:
        delta = EVENT_END_TIME - now
        seconds = delta.total_seconds()
        fg = YELLOW if seconds > 3600 else ORANGE
        crawl_messages.set_message(2, '%s ends in %s' % (EVENT_NAME, delta_time_to_string(delta)))
        crawl_messages.set_message_colors(2, fg, BLACK)
    else:
        crawl_messages.set_message(2, '%s is over.' % EVENT_NAME)
        crawl_messages.set_message_colors(2, RED, BLACK)


class CrawlMessages:
    """
    class to manage a crawl of varied text messages on the bottom of the display
    """

    def __init__(self, screen, size):
        self.screen = screen
        self.size = size
        self.messages = [''] * 10
        self.message_colors = [(GREEN, BLACK)] * 10
        self.message_surfaces = None
        self.last_added_index = -1
        self.first_x = -1

    def set_message(self, index, message):
        if index >= 0 and index < len(self.messages):
            self.messages[index] = message

    def set_message_colors(self, index, fg, bg):
        if index >= 0 and index < len(self.messages):
            self.message_colors[index] = (fg, bg)

    def crawl_message(self):
        if self.message_surfaces is None:
            self.message_surfaces = [view_font.render(' ' + self.messages[0] + ' ', True,
                                                      self.message_colors[0][0],
                                                      self.message_colors[0][1])]
            self.first_x = self.size[0]
            self.last_added_index = 0

        self.first_x -= 2  # JEFF
        rect = self.message_surfaces[0].get_rect()
        if self.first_x + rect.width < 0:
            self.message_surfaces = self.message_surfaces[1:]
            self.first_x = 0
        x = self.first_x
        for surf in self.message_surfaces:
            rect = surf.get_rect()
            x = x + rect.width

        while x < self.size[0]:
            self.last_added_index += 1
            if self.last_added_index >= len(self.messages):
                self.last_added_index = 0
            if self.messages[self.last_added_index] != '':
                surf = view_font.render(' ' + self.messages[self.last_added_index] + ' ', True,
                                        self.message_colors[self.last_added_index][0],
                                        self.message_colors[self.last_added_index][1])
                rect = surf.get_rect()
                self.message_surfaces.append(surf)
                x += rect.width

        x = self.first_x
        for surf in self.message_surfaces:
            rect = surf.get_rect()
            rect.bottom = self.size[1] - 1
            rect.left = x
            self.screen.blit(surf, rect)
            x += rect.width
            if x >= self.size[0]:
                break


def update_charts(q, event, size):
    try:
        os.nice(10)
    except AttributeError:
        logging.warn("can't be nice to windows")
    q.put((CRAWL_MESSAGE, 4, 'Chart engine starting...'))
    base_map = create_map()
    last_qso_timestamp = 0
    q.put((CRAWL_MESSAGE, 4, ''))

    try:
        while not event.is_set():
            t0 = time.time()
            last_qso_timestamp = load_data(size, q, base_map, last_qso_timestamp)
            t1 = time.time()
            delta = t1 - t0
            update_delay = DATA_DWELL_TIME - delta
            if update_delay < 0:
                update_delay = DATA_DWELL_TIME
            logging.debug('Next data update in %f seconds', update_delay)
            event.wait(update_delay)
    except Exception, e:
        logging.exception('Exception in update_charts', exc_info=e)
        q.put((CRAWL_MESSAGE, 4, 'Chart engine failed.', YELLOW, RED))


def change_image(screen, size, images, image_index, delta):
    while True:
        image_index += delta
        if image_index >= len(images):
            image_index = 0
        elif image_index < 0:
            image_index = len(images) - 1
        if images[image_index] is not None:
            break
    show_page(screen, size, images[image_index])
    return image_index


def main():
    logging.info('dashboard startup')
    last_qso_timestamp = 0
    q = multiprocessing.Queue()
    if 'HTML_ONLY' in globals():
        if HTML_ONLY:
            logging.info('HTML ONLY so no screen will appear')
            # Setup simple loop to call load_data and then wait for the interval
            base_map = create_map()
            last_qso_timestamp = 0
            if not ('PNG_HEIGHT' in globals() and 'PNG_WIDTH' in globals()):
                logging.info('PNG_HEIGHT and/or PNG_WIDTH not specified in config file - Using 800x600')
                size = (800, 600)
            else:
                size = (PNG_HEIGHT, PNG_WIDTH)

            run = True
            while run:
                # t0 = time.time()
                last_qso_timestamp = load_data(size, q, base_map, last_qso_timestamp)
                # t1 = time.time()

                while not q.empty():  # Empty queue even through we do not use it to prevent potential memory issues.
                    q.get_nowait()
                # delta = t1 - t0
                # update_delay = DATA_DWELL_TIME - delta
                # if update_delay < 0:
                #  update_delay = DATA_DWELL_TIME
                logging.debug('Next data update in %f seconds', DATA_DWELL_TIME)

                time.sleep(DATA_DWELL_TIME)
    # If HTML_ONLY, the rest of this code will never execute.        
    process_event = multiprocessing.Event()

    images = [None] * IMAGE_COUNT
    try:
        screen, size = init_display()
    except Exception, e:
        logging.exception('Could not initialize display.', exc_info=e)
        sys.exit(1)

    display_size = (size[0], size[1] - view_font_height)

    logging.debug('display setup')

    images[LOGO_IMAGE_INDEX] = pygame.image.load('logo.png')
    crawl_messages = CrawlMessages(screen, size)
    update_crawl_message(crawl_messages)

    proc = multiprocessing.Process(name='image-updater', target=update_charts, args=(q, process_event, display_size))
    proc.start()

    try:
        image_index = LOGO_IMAGE_INDEX
        show_page(screen, size, images[LOGO_IMAGE_INDEX])

        pygame.time.set_timer(pygame.USEREVENT, 1000)
        run = True
        paused = False

        display_update_timer = DISPLAY_DWELL_TIME
        clock = pygame.time.Clock()

        while run:
            for event in pygame.event.get():
                if event.type == pygame.QUIT:
                    run = False
                    break
                elif event.type == pygame.USEREVENT:
                    display_update_timer -= 1
                    if display_update_timer < 1:
                        if paused:
                            show_page(screen, size, images[image_index])
                        else:
                            image_index = change_image(screen, size, images, image_index, 1)
                        display_update_timer = DISPLAY_DWELL_TIME
                    update_crawl_message(crawl_messages)
                elif event.type == pygame.KEYDOWN:
                    if event.key == ord('q'):
                        logging.debug('Q key pressed')
                        run = False
                    elif event.key == ord('n') or event.key == 275:
                        logging.debug('next key pressed')
                        image_index = change_image(screen, size, images, image_index, 1)
                        display_update_timer = DISPLAY_DWELL_TIME
                    elif event.key == ord('p') or event.key == 276:
                        logging.debug('prev key pressed')
                        image_index = change_image(screen, size, images, image_index, -1)
                        display_update_timer = DISPLAY_DWELL_TIME
                    elif event.key == 302:
                        logging.debug('scroll lock key pressed')
                        if paused:
                            image_index = change_image(screen, size, images, image_index, 1)
                            display_update_timer = DISPLAY_DWELL_TIME
                        paused = not paused
                    else:
                        logging.debug('event key=%d', event.key)
                while not q.empty():
                    payload = q.get()
                    message_type = payload[0]
                    if message_type == IMAGE_MESSAGE:
                        n = payload[1]
                        image = payload[2]
                        image_size = payload[3]
                        images[n] = pygame.image.frombuffer(image, image_size, IMAGE_FORMAT)
                        logging.debug('received image %d', n)
                    elif message_type == CRAWL_MESSAGE:
                        n = payload[1]
                        message = payload[2]
                        fg = CYAN
                        bg = BLACK
                        if len(payload) > 3:
                            fg = payload[3]
                        if len(payload) > 4:
                            bg = payload[4]
                        crawl_messages.set_message(n, message)
                        crawl_messages.set_message_colors(n, fg, bg)

            crawl_messages.crawl_message()
            pygame.display.flip()

            clock.tick(60)  # JEFF

        pygame.time.set_timer(pygame.USEREVENT, 0)
    except Exception, e:
        logging.exception("Exception in main:", exc_info=e)

    pygame.display.quit()
    logging.debug('stopping update process')
    process_event.set()
    logging.debug('waiting for update process to stop...')
    proc.join(60)
    if proc.is_alive():
        logging.warn('chart engine did not exit upon request, killing.')
        proc.terminate()
    logging.debug('update thread has stopped.')
    logging.info('dashboard exit')


if __name__ == '__main__':
    main()
