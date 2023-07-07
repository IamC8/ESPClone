from datetime import datetime, timedelta
from pytz import timezone
import requests
import pickle
import json
import sqlite3
import hashlib
from bs4 import BeautifulSoup
import os
import sys
import logging.config


class ReadDB:
    def __init__(self):
        self.logger = LOG
        self.stage = [int(esp_status['status']['eskom']['stage']), int(esp_status['status']['capetown']['stage'])]

    def readDB(self, info, cursor):
        _format = "%Y-%m-%dT%H:%M:%S%z"
        try:
            self.logger.debug('Starting Collecting Timeslots')
            self_stage = self.stage
            stages_upcomming_events = []
            _eskom_empty = False
            _capetown_empty = False
            self.logger.debug('Current Stage: %s' % self_stage)
            _s1 = '"' + info[0] + '"'
            _s2 = '"' + info[1] + '"'
            sq = """
            SELECT s.aid, s.bagz, s.provider, s.province, s.muni, s.name, m.dc, m.source
            FROM subburbs s            
            INNER JOIN meta m ON m.aid = s.aid
            WHERE s.provider = ? AND s.name = ?
            LIMIT 1;        
            """
            cursor.execute(sq, info)
            _area_data = cursor.fetchall()
            if _area_data[0][0].replace(' ', '').lower() == 'eskomdirect2e':
                aid = 'eskde'
            elif _area_data[0][0].replace(' ', '').lower() == 'eskomdirect2u':
                aid = 'eskdu'
            else:
                aid = _area_data[0][0].replace(' ', '').lower()

            _esp_id = aid + '-' + _area_data[0][1].replace(' ', '').lower() + '-' + _area_data[0][5].replace(' ', '').lower() + _area_data[0][4].replace(' ', '').lower() + _area_data[0][3].replace(' ', '').lower()
            _sourced_info = _area_data[0][7]
            if _area_data:
                days = []
                for _itterate_days in range(0, 7):
                    stages_current_area = []
                    for _stage in range(1, 9):
                        _start_finish_times = []
                        _new_start_fin = []
                        _day = datetime.now() + timedelta(days=_itterate_days)
                        _dow = _day.strftime('%u')
                        _status_area = 'eskom'
                        if 'capetown' in info[0]:
                            _status_area = 'capetown'
                        if _area_data[0][6] == 'd':
                            _sql_search = 'aid == "' + _area_data[0][0] + '" and bagz == "' + str(
                                _area_data[0][1]) + '" and date_of_month == "' + str(
                                _day.day) + '" and stage == "' + str(
                                _stage) + '"'
                        else:
                            _sql_search = 'aid == "' + _area_data[0][0] + '" and bagz == "' + str(
                                _area_data[0][1]) + '" and cal_day == "' + str(
                                _dow) + '" and stage == "' + str(
                                _stage) + '"'
                        cursor.execute('SELECT * FROM main_areas WHERE ' + _sql_search)
                        _days = cursor.fetchall()
                        _timeslot = '[]'
                        if _days:
                            for _days_info in _days:
                                _timeslot = _days_info[5].strip() + '-' + _days_info[6].strip()
                                _start_finish_times.append(_timeslot)
                            _end_time = []
                            _start_time = []
                            for _overflow in _start_finish_times:
                                _overflow_start = _overflow.split('-')[0]
                                _overflow_end = _overflow.split('-')[1]
                                _start_time.append(int(_overflow_start.replace(':', '')))
                                _end_time.append(int(_overflow_end.replace(':', '')))
                            _start_fin_len = len(_start_time)
                            if _start_fin_len >= 2:
                                _new_start_fin.append(_start_finish_times[0])
                                for _range_cnt in range(1, _start_fin_len):
                                    try:
                                        if _end_time[_range_cnt - 1] > _start_time[_range_cnt]:
                                            _start_finish_str = _start_finish_times[_range_cnt - 1].split('-')[
                                                                    0] + '-' + \
                                                                _start_finish_times[_range_cnt].split('-')[1]
                                            _new_start_fin.pop()
                                            _new_start_fin.append(_start_finish_str)
                                        else:
                                            _new_start_fin.append(_start_finish_times[_range_cnt])
                                    except:
                                        pass
                            else:
                                if _overflow not in _new_start_fin:
                                    _new_start_fin.append(_start_finish_times[0])
                            stages_current_area.append(_new_start_fin)
                        else:
                            stages_current_area.append([])
                    days.append(
                        {'date': _day.strftime('%Y-%m-%d'), 'name': _day.strftime('%A'), 'stages': stages_current_area})
            timeslot_events = []
            for _d in days:
                _day_short_dt = datetime.strptime(_d['date'], '%Y-%m-%d')
                for _stage_cnt in range(1, 9):
                    _time_slots = _d['stages'][_stage_cnt - 1]
                    for _time_slot in _time_slots:
                        aaa = _time_slot.split('-')[1]
                        if ':30' in aaa:
                            aaa = aaa.replace(':30', ':00')
                        _time_slot_start_str = _d['date'] + 'T' + _time_slot.split('-')[0] + ':00+02:00'
                        _time_slot_end_str = _d['date'] + 'T' + _time_slot.split('-')[1] + ':00+02:00'
                        _time_slot_end_str2 = _d['date'] + 'T' + aaa + ':00+02:00'
                        _time_slot_start_dt = datetime.strptime(_time_slot_start_str, _format)
                        _time_slot_end_dt = datetime.strptime(_time_slot_end_str, _format)
                        _time_slot_end_dt2 = datetime.strptime(_time_slot_end_str2, _format)
                        if _time_slot_end_dt < _time_slot_start_dt:
                            _time_slot_end_dt = datetime.strptime(_time_slot_end_str, _format) + timedelta(days=1)
                            _time_slot_end_str = _time_slot_end_dt.strftime(_format)
                        if _time_slot_end_dt2 < _time_slot_start_dt:
                            _time_slot_end_dt2 = datetime.strptime(_time_slot_end_str2, _format) + timedelta(days=1)
                            _time_slot_end_str2 = _time_slot_end_dt2.strftime(_format)
                        _ts_event = {'Stage': _stage_cnt, 'Start': _time_slot_start_dt, 'End': _time_slot_end_dt}
                        timeslot_events.append(_ts_event)
            self.logger.debug('Finished Collecting Timeslots')
            try:
                if True:
                    _time_slots = []
                    _tz = timezone('Africa/Johannesburg')

                    if _area_data[0][1] == 'City of Cape Town':
                        stage = self_stage[1]
                        stages_upcomming_events = esp_status['status']['capetown']['next_stages']
                        _new_start_stage = esp_status['status']['capetown'][
                            'stage_updated'].split(
                            '.')
                        _new_start_stage = _new_start_stage[0] + '+0200'
                        _stages_upcomming_events = [{'stage': str(stage), 'stage_start_timestamp': _new_start_stage}]
                    else:
                        stage = self_stage[0]
                        stages_upcomming_events = esp_status['status']['eskom']['next_stages']
                        _new_start_stage = esp_status['status']['eskom']['stage_updated'].split(
                            '.')
                        _new_start_stage = _new_start_stage[0] + '+0200'
                        _stages_upcomming_events = [{'stage': str(stage), 'stage_start_timestamp': _new_start_stage}]

                    if len(stages_upcomming_events) == 0:
                        if _area_data[0][1] == 'City of Cape Town':
                            _capetown_empty = True
                        else:
                            _eskom_empty = True
                        for _dummy_cnt in range(1, 3):
                            _dummy_dt = datetime.strptime(_new_start_stage, _format) + timedelta(days=_dummy_cnt)
                            _dummy_dt = _dummy_dt.replace(minute=00)
                            _dummy_str = _dummy_dt.strftime(_format)
                            stages_upcomming_events.append({'stage': str(stage), 'stage_start_timestamp': _dummy_str})
                    self.logger.debug('Area: %s' % _area_data[0][1])
                    self.logger.debug('Starting Collecting Events')
                    stages_upcomming_events = _stages_upcomming_events + stages_upcomming_events
                    for _ss in range(0, len(stages_upcomming_events)):
                        stages_upcomming_events[_ss]['start'] = stages_upcomming_events[_ss]['stage_start_timestamp']
                        stages_upcomming_events[_ss]['Stage'] = int(stages_upcomming_events[_ss]['stage'])
                    _stages_upcomming_events = []
                    try:
                        for _dx in range(0, len(stages_upcomming_events)):
                            _stages_upcomming_events.append(stages_upcomming_events[_dx])
                            _stages_upcomming_events[_dx]['end'] = stages_upcomming_events[_dx + 1][
                                'start']
                            _stages_upcomming_events[_dx]['Start'] = \
                                datetime.strptime(_stages_upcomming_events[_dx]['start'], _format)
                            _stages_upcomming_events[_dx]['End'] = \
                                datetime.strptime(_stages_upcomming_events[_dx]['end'], _format)
                    except:
                        _dt_str = (datetime.strptime(stages_upcomming_events[_dx]['start'], _format)
                                   + timedelta(days=7)).replace(hour=00).replace(minute=00).replace(
                            microsecond=00).strftime(_format)
                        _stages_upcomming_events[_dx]['end'] = _dt_str
                        _stages_upcomming_events[_dx]['Start'] = \
                            datetime.strptime(_stages_upcomming_events[_dx]['start'], _format)
                        _stages_upcomming_events[_dx]['End'] = \
                            datetime.strptime(_stages_upcomming_events[_dx]['end'], _format)
                    stages_upcomming_events = _stages_upcomming_events
            except Exception as err:
                self.logger.error(err)
            self.logger.debug('Finished Collecting Events')
            _col_events_str = []
            _col_events_dic = []
            self.logger.debug('Timeslots found: %s' % len(timeslot_events))
            self.logger.debug('Events found: %s' % len(stages_upcomming_events))
            for _check_days in timeslot_events:
                for _check_event in stages_upcomming_events:
                    if _check_days['Stage'] == _check_event['Stage']:
                        try:
                            if _check_days['Start'] >= _check_event['Start'] \
                                    and _check_days['End'] <= _check_event['End']:
                                _append = str(_check_days['Stage']) + str(_check_days['Start'])
                                if _append not in _col_events_str:
                                    _col_events_str.append(_append)
                                    _time_date = str(_check_days['Start'].date())
                                    _time_start = str(_check_days['Start'].time()).replace(':00:00', ':00')
                                    _time_end = str(_check_days['End'].time()).replace(':30:00', ':00')
                                    _tst_split_start = _time_start.split(':')
                                    if len(_tst_split_start) > 2:
                                        _time_start = ':'.join(_tst_split_start[:2])
                                    _tst_split_end = _time_end.split(':')
                                    if len(_tst_split_end) > 2:
                                        _time_end = ':'.join(_tst_split_end[:2])
                                    _col_events_dic.append({'Stage': _check_days['Stage'],
                                                            'Start': _check_days['Start'],
                                                            'End': _check_days['End'],
                                                            'Str': 'Stage: '
                                                                   + str(_check_days['Stage'])
                                                                   + ' ' + _time_date + ' '
                                                                   + _time_start + ' - ' + _time_end})
                            elif _check_event['Start'] <= _check_days['Start'] < _check_event['End'] < \
                                    _check_days[
                                        'End']:
                                _append = str(_check_days['Stage']) + str(_check_days['Start'])
                                if _append not in _col_events_str:
                                    _col_events_str.append(_append)
                                    _time_date = str(_check_days['Start'].date())
                                    _time_start = str(_check_days['Start'].time()).replace(':00:00', ':00')
                                    _time_end = str(_check_event['End'].time()).replace(':00:00', ':00')
                                    _tst_split_start = _time_start.split(':')
                                    if len(_tst_split_start) > 2:
                                        _time_start = ':'.join(_tst_split_start[:2])
                                    _tst_split_end = _time_end.split(':')
                                    if len(_tst_split_end) > 2:
                                        _time_end = ':'.join(_tst_split_end[:2])
                                    _col_events_dic.append({'Stage': _check_days['Stage'],
                                                            'Start': _check_days['Start'],
                                                            'End': _check_event['End'],
                                                            'Str': 'Stage: '
                                                                   + str(_check_days['Stage'])
                                                                   + ' ' + _time_date + ' '
                                                                   + _time_start + ' - ' + _time_end})
                            elif _check_days['Start'] < _check_event['Start'] < _check_days['End']:
                                _append = str(_check_days['Stage']) + str(_check_event['Start'])
                                if _append not in _col_events_str:
                                    _col_events_str.append(_append)
                                    _time_date = str(_check_event['Start'].date())
                                    _time_start = str(_check_event['Start'].time()).replace(':00:00', ':00')
                                    _time_end = str(_check_days['End'].time()).replace(':30:00', ':00')
                                    _tst_split_start = _time_start.split(':')
                                    if len(_tst_split_start) > 2:
                                        _time_start = ':'.join(_tst_split_start[:2])
                                    _tst_split_end = _time_end.split(':')
                                    if len(_tst_split_end) > 2:
                                        _time_end = ':'.join(_tst_split_end[:2])
                                    _col_events_dic.append({'Stage': _check_days['Stage'],
                                                            'Start': _check_event['Start'],
                                                            'End': _check_days['End'],
                                                            'Str': 'Stage: '
                                                                   + str(_check_days['Stage'])
                                                                   + ' ' + _time_date + ' '
                                                                   + _time_start + ' - ' + _time_end})
                        except:
                            pass

            _tmp_all_events = sorted(_col_events_dic, key=lambda i: i['Start'], reverse=False)
            all_events = []
            for _data in _tmp_all_events:
                if _data['Start'].hour != _data['End'].hour:
                    all_events.append({'Stage': _data['Stage'],
                                       'stage': str(_data['Stage']),
                                       'stage_start_timestamp': _data['Start'],
                                       'stage_end_timestamp': _data['End'],
                                       'event_info': _data['Str']
                                       })
            _region = _area_data[0][1] + ', ' + _area_data[0][4] + ', ' + _area_data[0][5]
            _region = ' '.join(_region.split(','))
            _region = ' '.join(_region.split())
            return_data = {'id': _esp_id, 'events': all_events,
                           'info': {'name': _area_data[0][0],
                                    'bagz': _area_data[0][1]},
                           'schedule': {'days': days, 'source': _sourced_info}}
            self.logger.debug('Finished')
            self.logger.debug('Events Found')
            return return_data
        except Exception as err:
            self.logger.error('ERROR: %s' % err)


class GetStatusFromESP:
    def __init__(self):
        self.logger = LOG
        self.logger.debug('GetStatusFromESP')
        token = os.environ.get('ESP_TOKEN')
        self.payload = {}
        self.headers = {'Token': token}

    def get_resp(self, r):
        resp_json = ''
        if r.status_code == 200:
            try:
                resp_json = r.json()
            except:
                ...
        else:
            self.logger.error('Error in getting response: %s ' % r.text)
            exit(9)

        return resp_json

    def check_allowance_esp(self):
        url = "https://developer.sepush.co.za/business/2.0/api_allowance"
        r = requests.get(url, headers=self.headers, data=self.payload)
        return self.get_resp(r)

    def get_status_from_esp(self):
        url = "https://developer.sepush.co.za/business/2.0/status"
        r = requests.request("GET", url, headers=self.headers, data=self.payload)
        return self.get_resp(r)

    def main(self):
        self.logger.debug('GetStatusFromESP')
        res = self.check_allowance_esp()
        if res['allowance']['count'] < res['allowance']['limit']:
            self.logger.debug('%s of %s tokens used.' % (res['allowance']['count'], res['allowance']['limit']))
            self.logger.info('Using 1 Token from ESP')
            esp_status = self.get_status_from_esp()
            pickle.dump(esp_status, open('status_info.pickle', "wb"))
        else:
            self.logger.error('Tokens depleted')
            sys.exit(9)
        return esp_status


class CheckStatusChanged:
    def __init__(self):
        self.url_coc = 'https://www.capetown.gov.za/loadshedding'
        self.url_eskom = 'https://www.eskom.co.za'
        self.logger = LOG
        self.logger.debug('GetStatusChanges')

    def coc(self):
        data = requests.get(self.url_coc)
        soup = BeautifulSoup(data.text, 'html.parser')
        ls = soup.find_all('div', class_='section-pull')
        result = ls[0].text
        res = result.split('Follow')
        res = res[0].split('customers:')
        inserts = ['', 'City customers:', 'Eskom customers:', '']
        pointer = 0
        msg = []
        for info in res:
            pops = []
            _r = info.split('\n')
            for i in range(0, len(_r)):
                _c = _r[i].replace('\r', '').replace('\t', '')
                if _c == "":
                    pops.append(i)
                else:
                    _r[i] = _c
            pops.sort(reverse=True)
            for p in pops:
                _r.pop(p)
            _r.insert(0, inserts[pointer])
            pointer = pointer + 1
            msg.append(_r[:-1])
        msg.pop(0)
        msg_str = ""
        for m in msg:
            for _m in m:
                msg_str = msg_str + _m + '\n'
        msg_md5 = hashlib.md5(msg_str.encode('utf-8')).hexdigest()
        msg.append(msg_md5)
        try:
            prev_msg = pickle.load(open('capetown.pickle', 'rb'))
            prev_md5 = prev_msg[-1]
        except:
            prev_md5 = "AAAAAAAAAAAAAA"
        changed = False
        if msg_md5 != prev_md5:
            pickle.dump(msg, open('capetown.pickle', "wb"))
            self.logger.debug('CoC changed')
            changed = True
        return msg, changed

    def eskom(self):
        msg = []
        data = requests.get(self.url_eskom)
        soup = BeautifulSoup(data.text, 'html.parser')
        ls = soup.find_all('div', class_='elementor-alert elementor-alert-danger')
        res = ls[0].text
        msg_md5 = hashlib.md5(res.encode('utf-8')).hexdigest()
        msg.append(res)
        msg.append(msg_md5)
        try:
            prev_msg = pickle.load(open('eskom.pickle', 'rb'))
            prev_md5 = prev_msg[-1]
        except:
            prev_md5 = "AAAAAAAAAAAAAA"
        changed = False
        if msg_md5 != prev_md5:
            pickle.dump(msg, open('eskom.pickle', "wb"))
            self.logger.debug('Eskom changed')
            changed = True
        return msg, changed

    def main(self):
        self.logger.debug('CheckStatusChanged - main')
        _e, c1 = self.eskom()
        _c, c2 = self.coc()
        if c1 or c2:
            self.logger.info('Status Changed')
            esp_status = GetStatusFromESP().main()
        else:
            try:
                esp_status = pickle.load(open('status_info.pickle', 'rb'))
                self.logger.info('Status Unchanged')
            except:
                esp_status = GetStatusFromESP().main()
        return esp_status


if __name__ == "__main__":
    current_path = os.path.dirname(os.path.realpath(sys.argv[0]))
    DATADIR = current_path + '/data'
    try:
        os.chdir(DATADIR)
    except:
        os.mkdir(DATADIR)
        os.chdir(DATADIR)
    NEW_FIN_DB = 'loadshedding.db'
    CLIENT_NAME = 'Loadshedding'
    LOGNAME = CLIENT_NAME
    LOCALLOG = '/tmp/' + CLIENT_NAME + '.log'
    log = logging.getLogger(CLIENT_NAME)
    log.setLevel(logging.DEBUG)
    formatter = logging.Formatter(fmt="%(asctime)s %(levelname)s: %(message)s",
                                  datefmt="%Y-%m-%d - %H:%M:%S")
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)
    fh = logging.FileHandler(LOCALLOG, "a")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    log.addHandler(ch)
    log.addHandler(fh)
    LOG = log

    esp_status = CheckStatusChanged().main()
    search_provider = 'Eskom'
    search_term = 'Leeukop'
    _conn = sqlite3.connect(NEW_FIN_DB)
    cursor = _conn.cursor()
    _src = (search_provider, search_term)
    return_data = ReadDB().readDB(_src, cursor)
    cursor.close()
    print(json.dumps(return_data, default=str, indent=2))


