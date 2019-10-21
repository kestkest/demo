# coding: utf-8
from __future__ import division
import datetime
import json
import copy
import itertools
import hashlib

from mongoengine import Q
from leadhit_common.functions import get_minimal_url
from django.conf import settings
from django.utils.translation import ugettext, ugettext_lazy as _

from bson import ObjectId
from bson.dbref import DBRef
from accounts.models import YMLOffer, YMLFile

from automailer.models import Autocast
from leads.models import Lead, TraffSource, LeadEvent, LeadOrder, Multilead, CartItem
from analytics.utils.helpers import Period, validate_time_period, get_sum_orders_by_week
from analytics.forms import WidgetStatForm, StatForm
from analytics.utils.helpers import humanize_form_errors
from accounts.models import WidgetsConf
from emails.models import Message, Email, UnsubscribedEmail
from widgets.helpers import get_recommendations_widgets

db = settings.DB
widgets_with_conversion = ('New_Smart_final', 'wish_list')


class Chart(object):
    """
        Base Chart class. Contains bwish_listasic chart settings and a 'period' helper.
        Each subclass has to declare 'events' attribute in order to set proper settings for axes and graphs.
        The 'events' attr is declared this way:
        events = {
            {event1: {name: event1_name, verbose_name: event1_verbose_name, color: event1_color}}
            {event2: {name: event2_name, verbose_name: event2_verbose_name, color: event2_color}}
        }
    """
    def __init__(self):

        self.period = Period()
        self.chart_settings = {
            "type": "serial",
            "theme": "light",
            "marginRight": 80,
            "autoMarginOffset": 20,
            "marginTop": 7,
            "synchronizeGrid": True,
            "legend": {
                "useGraphSettings": True,
            },
            "mouseWheelZoomEnabled": True,
            "categoryAxis": {
                "parseDates": True,
                "axisColor": "#DADADA",
                "minorGridEnabled": True,
                "minPeriod": 'mm'
            },
            "chartScrollbar": {},
            "chartCursor": {
                "cursorPosition": "mouse",
                "pan": True,
            },
            "export": {
                "enabled": True,
                "position": "bottom-right"
            },
            # This is a path to the static files used by amcharts. It depends on a webpack configuration of the 'CopyWebpackPlugin' plugin.
            "path": "/static/dist/amcharts",

            # Since we only use 'dates' in our stats categoryField value is set to 'date' by default
            "categoryField": "date",

            # valueAxes list depends on "events" and gets generated depending on event's values.
            # Is set to None to avoid caching.
            "valueAxes": None,

            # The graph list depends on "events" and gets generated depending on event's values
            # Is set to None to avoid caching
            "graphs": None,
            "dataDateFormat": "YYYY-MM-DD HH:NN",
        }
        # Axes options.
        # In case of multiple graphs we instantiate multiple axes e.g chart_settings['valueAxes'] = [axis1_settings, axis2_settings, etc]
        # Values:
        # id - has to be set in case of multiple graphs. Axis id is used by a certain graph e.g. graph['valueAxis'] = axis['id']
        # title and axisColor are self explanatory.
        self.axis_settins = {
            "id": None,
            "axisColor": '#787978',
            "axisThickness": 2,
            "axisAlpha": 1,
            "position": "left",
            "title": None,
        }
        # Graph's options.
        # Values to be set:
        #   valueAxis - binds graph to a particular axis (relevant if multiple axes are set),
        #   title - this attr gets named after event's verbose name,
        #   valueField - a field from dataProvider object to get data from
        #   lineColor - graph color :)
        self.graph_settings = {
            "valueAxis": None,
            "balloonText": "[[value]]",
            "bullet": "round",
            "bulletBorderAlpha": 1,
            "bulletColor": "#FFFFFF",
            "hideBulletsCount": 50,
            "title": None,
            "valueField": None,
            "useLineColorForBulletBorder": True,
            "balloon": {
                "drop": True
            },
            "bulletSize": 3,
            "lineThickness": 2,
            "lineColor": None,
            "fillAlphas": 0,
        }

    def generate_graphs_and_axes(self, events):

        axis_offset = 0
        axis_counter = 0
        axis_placement = 'left'

        axes = []
        graphs = []

        for event in events:
            axis = self.axis_settins.copy()
            graph = self.graph_settings.copy()

            axis_placement = 'left' if axis_counter % 2 == 0 else 'right'

            axis_offset = axis_counter / 2 * 40

            if axis_counter >= 2:
                axis["gridAlpha"] = 0

            event_data = self.events.get(event)

            axis["id"] = event_data["name"]
            axis["axisColor"] = event_data["color"]
            axis["position"] = axis_placement
            axis["offset"] = axis_offset

            graph["id"] = event_data["name"]
            graph["valueField"] = event_data["name"]
            graph["title"] = event_data["verbose_name"]
            graph["lineColor"] = event_data["color"]
            graph["valueAxis"] = axis["id"]

            axis_counter += 1

            axes.append(axis)
            graphs.append(graph)

        return {'axes': axes, 'graphs': graphs}

    def get_data(self):
        raise NotImplementedError("get_data is not implemented yet")

    def validate_input(self, data):
        raise NotImplementedError("validate_input is not implemented yet")

    def get_relevant_events(self, options):
        raise NotImplementedError("Should be implemented to generate graphs and axes")


class WidgetsChart(Chart):

    events = {
        'fill': {'name': 'fill', 'verbose_name': ugettext(u'Заполнения'), 'color': '#35b54b'},
        'view': {'name': 'view', 'verbose_name': ugettext(u'Показы'), 'color': '#787978'},
        'popup_view': {'name': 'popup_view', 'verbose_name': ugettext(u'Показы всплывающего окна'), 'color': '#2491c5'},
        'close': {'name': 'close', 'verbose_name': ugettext(u'Закрытия'), 'color': "#bd3c3c"},
        # conversion is a computed event: fill_count * 100 / popup_view
        'conversion': {'name': 'conversion', 'verbose_name': ugettext(u'Конверсия в среднем за период (%)'), 'color': '#4070a0'},
        'popup_view_3s': {'name': 'popup_view_3s', 'verbose_name': ugettext(u'Показы всплывающего окна 3 сек.'), 'color': '#ffff00'},
        'popup_view_10s': {'name': 'popup_view_10s', 'verbose_name': ugettext(u'Показ всплывающего окна 10 сек.'), 'color': '#66ff66'},
        'click': {'name': 'click', 'verbose_name': ugettext(u'Клики'), 'color': '#00b359'},
    }

    disabled_graphs = ('conversion', 'close', 'popup_view_10s', 'popup_view_3s')

    def validate_input(self, options):
        widget_id = options['widget_id']
        aggregate_period = options['aggregate_period']

        if options['period'] != 'custom':
            self.period = Period.from_def_ranges(options['period'])
            start = self.period.start
            end = self.period.end
        else:
            form = WidgetStatForm(options)
            if not form.is_valid():
                return {'status': 'error', 'errors': humanize_form_errors(form)}

            form_data = form.cleaned_data

            if not form_data['start']:
                form.errors['start'] = [_(u"Обязательное поле")]
                return {'status': 'error', 'errors': humanize_form_errors(form)}

            if not form_data['end']:
                form.errors['end'] = [_(u"Обязательное поле")]
                return {'status': 'error', 'errors': humanize_form_errors(form)}

            start = form_data['start']
            end = form_data['end'].replace(hour=23, minute=59, second=59)

            if start.date() > datetime.datetime.now().date():
                form.errors['start'] = [_(u"Начало периода не может быть позже текущего дня.")]
                return {'status': 'error', 'errors': humanize_form_errors(form)}
            if end.date() > datetime.datetime.now().date():
                form.errors['end'] = [_(u"Конец периода не может быть позже текущего дня.")]
                return {'status': 'error', 'errors': humanize_form_errors(form)}
            if start > end:
                form.errors["start"] = [_(u'Начало периода не должно быть больше конца периода.')]
                return {'status': 'error', 'errors': humanize_form_errors(form)}

        return {
            "status": "ok",
            "wid": widget_id,
            "start": start,
            "end": end,
            "aggregate_period": aggregate_period,
            "group_by_visits": options["group_by_visits"],
            "single_axis": options["single_axis"],
        }

    def get_relevant_events(self, options):

        site = ObjectId(self.request.site_id)

        lead_events = db.lead_events.find(
            {
                "init_by": options["wid"],
                "time_added": {
                    "$gte": options["start"],
                    "$lte": options["end"],
                },
                'site': DBRef("sites", site),
                "category": "widgets",
                "event_type": {"$in": self.events.keys()}
            }).distinct("event_type")

        aggr_events = db.aggregated_events.find(
            {
                "init_by": options["wid"],
                "time_added": {
                    "$gte": options["start"],
                    "$lte": options["end"],
                },
                'site': DBRef("sites", site),
                "category": "widgets",
                "event_type": {"$in": self.events.keys()}
            }).distinct("event_type")

        return list(set(lead_events) | set(aggr_events))

    def get_data(self, options):

        relevant_events = self.get_relevant_events(options)

        data = self.build_query_and_get_data(relevant_events, options)

        widget_type = self.get_widget_type(options['wid'])

        if widget_type in widgets_with_conversion:
            relevant_events.append(self.events['conversion']['name'])
            graph_data = self.build_graph_data(data, conversion=True)
            total_values_per_event = self.get_total_per_event(graph_data, conversion=True)
        else:
            graph_data = self.build_graph_data(data)
            total_values_per_event = self.get_total_per_event(graph_data)

        graphs_and_axes = self.generate_graphs_and_axes(relevant_events)
        self.chart_settings["graphs"] = graphs_and_axes['graphs']
        if options['single_axis'] == 'false':
            self.chart_settings["valueAxes"] = graphs_and_axes["axes"]
        else:
            self.chart_settings['valueAxes'] = [self.axis_settins]

        if options['aggregate_period'] == 'hour':
            self.chart_settings["categoryAxis"]["minPeriod"] = 'mm'
        if options['aggregate_period'] == 'day':
            self.chart_settings["categoryAxis"]["minPeriod"] = 'DD'
        if options['aggregate_period'] == 'month':
            self.chart_settings["categoryAxis"]["minPeriod"] = 'MM'

        return {
            "status": "ok",
            "graph": graph_data,
            'options': self.chart_settings,
            'total_values_per_event': total_values_per_event,
            'start': options['start'].date().strftime('%d-%m-%Y'),
            'end': options['end'].date().strftime('%d-%m-%Y'),
            'disabled_graphs': self.disabled_graphs,
        }

    def build_graph_data(self, data, **options):
        graph_data = []
        if options.get('conversion'):
            for period in data:
                temp = {}
                temp['date'] = period
                for event in data[period]:
                    if event['event_type'] not in temp:
                        temp[event['event_type']] = event['count']
                    else:
                        temp[event['event_type']] += event['count']
                temp["conversion"] = round(temp.get('fill', 0) * 100 / temp.get("popup_view", 1), 2)
                graph_data.append(temp)
        else:
            for period in data:
                temp = {}
                temp['date'] = period
                for event in data[period]:
                    if event['event_type'] not in temp:
                        temp[event['event_type']] = event['count']
                    else:
                        temp[event['event_type']] += event['count']
                graph_data.append(temp)

        graph_data = sorted(graph_data, key=lambda x: x["date"])

        return graph_data

    def get_widget_type(self, wid):

        site = ObjectId(self.request.site_id)

        widgets_confs = WidgetsConf.objects.get(site=site)
        json_conf = json.loads(widgets_confs.config)['widgets']
        widget_type = json_conf[wid]['type']

        return widget_type

    def get_total_per_event(self, graph_data, **options):

        if not graph_data:
            return {}

        total_values_per_event = {}
        for date in graph_data:
            for event in date:
                if event == 'date':
                    continue
                event_name = self.events.get(event)['verbose_name']
                if event_name not in total_values_per_event:
                    total_values_per_event[event_name] = date[event]
                else:
                    total_values_per_event[event_name] += date[event]
        if options.get('conversion'):
            conversion_name = self.events['conversion']['verbose_name']
            fill = total_values_per_event.get(self.events.get('fill')['verbose_name'], 0)
            popup_view = total_values_per_event[self.events.get('popup_view')['verbose_name']]
            total_values_per_event[conversion_name] = round(fill / popup_view * 100, 2)
            return total_values_per_event
        return total_values_per_event

    def build_query_and_get_data(self, relevant_events, options):

        site = ObjectId(self.request.site_id)
        aggr_period = options['aggregate_period']

        group_by_periods = {
            'hour': {
                'all_events': {
                    'year': {'$substr': ['$time_added', 0, 4]},
                    'month': {'$substr': ['$time_added', 5, 2]},
                    'day': {'$substr': ['$time_added', 8, 2]},
                    'hour': {'$substr': ['$time_added', 11, 2]},
                    'event_type': '$event_type'
                },
                'unique_events': {
                    'year': {'$substr': ['$time_added', 0, 4]},
                    'month': {'$substr': ['$time_added', 5, 2]},
                    'day': {'$substr': ['$time_added', 8, 2]},
                    'hour': {'$substr': ['$time_added', 11, 2]},
                    'event_type': '$_id.event_type'
                },
            }
        }

        concat_by_periods = {
            'hour': {
                '$concat': ['$_id.year', '-', '$_id.month', '-', '$_id.day', ' ', '$_id.hour', ':00']
            },
            'day': {
                '$concat': ['$_id.year', '-', '$_id.month', '-', '$_id.day']
            },
            'month': {
                '$concat': ['$_id.year', '-', '$_id.month']
            }
        }

        day_grouping = copy.deepcopy(group_by_periods['hour'])
        # here we just remove grouping by hour
        del day_grouping['all_events']['hour']
        del day_grouping['unique_events']['hour']

        group_by_periods['day'] = day_grouping

        month_grouping = copy.deepcopy(group_by_periods['day'])
        # here we just remove grouping by day
        del month_grouping['all_events']['day']
        del month_grouping['unique_events']['day']

        group_by_periods['month'] = day_grouping

        if options['group_by_visits'] == 'false':
            match_stage = {
                "$match": {
                    "init_by": options["wid"],
                    'site': DBRef("sites", site),
                    "time_added": {
                        "$gte": options["start"],
                        "$lte": options["end"],
                    },
                    "category": "widgets",
                    "event_type": {"$in": relevant_events}
                }
            }

            project_stage = {
                '$project': {
                    'event_type': 1,
                    'time_added': 1,
                    'count': 1
                }
            }

            group_stage = {
                '$group': {
                    '_id': group_by_periods[aggr_period]['all_events'],
                    'count': {'$sum': 1}
                }
            }

            group_stage_aggregated = {
                '$group': {
                    '_id': group_by_periods[aggr_period]['all_events'],
                    'count': {'$sum': '$count'}
                }
            }

            second_group_stage = {
                '$group': {
                    '_id': concat_by_periods[aggr_period],
                    'events': {'$push': {'event_type': '$_id.event_type', 'count': '$count'}}
                },
            }

            lead_events = db.lead_events.aggregate([match_stage, project_stage, group_stage, second_group_stage])
            aggregated_events = db.aggregated_events.aggregate([match_stage, project_stage, group_stage_aggregated, second_group_stage])

            data = {}

            for item in lead_events:
                data[item['_id']] = item['events']

            for item in aggregated_events:
                if item['_id'] not in data:
                    data[item['_id']] = item['events']
                else:
                    data[item['_id']].extend(item['events'])
            return data
        else:
            match_stage = {
                "$match": {
                    'site': DBRef("sites", site),
                    "init_by": options["wid"],
                    "time_added": {
                        "$gte": options["start"],
                        "$lte": options["end"],
                    },
                    "category": "widgets",
                    "event_type": {"$in": relevant_events},
                    "visit": {"$exists": True}
                }
            }

            sort_stage = {
                "$sort": {
                    "time_added": 1
                }
            }

            first_group_stage = {
                "$group": {
                    "_id": {"visit": "$visit", "event_type": "$event_type"},
                    "time_added": {"$first": "$time_added"},
                }
            }

            second_group_stage = {
                '$group': {
                    '_id': group_by_periods[aggr_period]['unique_events'],
                    'count': {'$sum': 1}
                }
            }

            third_group_stage = {
                '$group': {
                    '_id': concat_by_periods[aggr_period],
                    'events': {'$push': {'event_type': '$_id.event_type', 'count': '$count'}}
                }
            }

            lead_events = db.lead_events.aggregate([match_stage, sort_stage, first_group_stage,
                                                    second_group_stage, third_group_stage])

            data = {}
            for item in lead_events:
                data[item['_id']] = item['events']
            return data


class LeadsChart(Chart):
    events = {
        'leads_added': {'name': 'leads_added', 'verbose_name': ugettext(u'Появилось клиентов'), 'color': '#168de2'},
    }

    def validate_input(self, options):
        if options['period'] != 'custom':
            self.period = Period.from_def_ranges(options['period'])
            start = self.period.start
            end = self.period.end
        else:
            form = StatForm(options)
            if not form.is_valid():
                return {'status': 'error', 'errors': humanize_form_errors(form)}
            form_data = form.cleaned_data

            if not form_data['period_start']:
                form.errors['period_start'] = [_(u"Обязательное поле")]
                return {'status': 'error', 'errors': humanize_form_errors(form)}

            if not form_data['period_end']:
                form.errors['period_end'] = [_(u"Обязательное поле")]
                return {'status': 'error', 'errors': humanize_form_errors(form)}

            start = form_data['period_start']
            end = form_data['period_end'].replace(hour=23, minute=59, second=59)

            if start.date() > datetime.datetime.now().date():
                form.errors['period_start'] = [_(u"Начало периода не может быть позже текущего дня.")]
                return {'status': 'error', 'errors': humanize_form_errors(form)}
            if end.date() > datetime.datetime.now().date():
                form.errors['period_end'] = [_(u"Конец периода не может быть позже текущего дня.")]
                return {'status': 'error', 'errors': humanize_form_errors(form)}
            if start > end:
                form.errors["period_start"] = [_(u'Начало периода не должно быть больше конца периода.')]
                return {'status': 'error', 'errors': humanize_form_errors(form)}
            self.period.start = start
            self.period.end = end
        return {
            'status': 'ok',
            'start': start,
            'end': end,
            'source': options['source']
        }

    def get_data(self, options, site=None):
        data = self.build_query_and_get_data(options, site=site)
        graphs_and_axes = self.generate_graphs_and_axes(['leads_added'])
        self.chart_settings['valueAxes'] = graphs_and_axes['axes']
        self.chart_settings['graphs'] = graphs_and_axes['graphs']
        self.chart_settings['dataProvider'] = data
        total_leads_added = sum([item['leads_added'] for item in data])
        return {
            'status': 'ok',
            'chart_settings': self.chart_settings,
            'step': self.period.step,
            'total_leads_added': total_leads_added
        }

    def build_query_and_get_data(self, options, site=None):
        # ugly adhoc should have been reworked
        site = site if site else self.request.site

        data = []
        start = options['start']
        end = options['end']
        if options['source'] == 'all':
            dates = Lead.objects(time_added__gte=start,
                                 time_added__lte=end,
                                 site=self.request.site).order_by('time_added').values_list('time_added')
        else:
            source = TraffSource.objects.get(name=options['source'])
            dates = Lead.objects(time_added__gte=start,
                                 time_added__lte=end,
                                 source__in=source.domains,
                                 site=self.request.site).values_list('time_added')
        if self.period.step == 'day':
            dates = [time_added.date().strftime('%Y-%m-%d') for time_added in dates]
            temp_dict = {}
            for date in dates:
                if date not in temp_dict:
                    temp_dict[date] = 1
                else:
                    temp_dict[date] += 1

            data = [{'date': k, 'leads_added': v} for k, v in temp_dict.iteritems()]
            data = sorted(data, key=lambda item: item['date'])
        else:
            self.chart_settings['categoryAxis']['minPeriod'] = 'mm'
            dates = [time_added.strftime("%Y-%m-%d %H:00") for time_added in dates]
            temp_dict = {}
            for date in dates:
                if date not in temp_dict:
                    temp_dict[date] = 1
                else:
                    temp_dict[date] += 1
            data = [{'date': k, 'leads_added': v} for k, v in temp_dict.iteritems()]
            data = sorted(data, key=lambda item: item['date'])
        return data


class FunnelChart(object):
    def __init__(self):
        self.chart_settings = {
            "type": "funnel",
            "theme": "light",
            "balloon": {
                "fixedPosition": True
            },
            "valueField": "value",  # Event value
            "titleField": "title",  # Event name
            "marginRight": 240,
            "marginLeft": 50,
            "startX": -500,
            "depth3D": 100,
            "angle": 25,
            "outlineAlpha": 1,
            "outlineColor": "#FFFFFF",
            "outlineThickness": 2,
            "labelPosition": "right",
            "balloonText": "[[title]]: [[value]] [[description]]",
            "export": {
                "enabled": True
            },
            "path": "/static/dist/amcharts",

            "fontSize": 14,
            "descriptionField": "description"
        }


class EmailCampaignsChart(FunnelChart):

    def validate_input(self, options):
        ids = options.getlist('ids[]')
        query = {}
        if not ids:
            return {'status': 'empty'}

        query['ids'] = ids

        if options['mailing_type'] == 'autocast':
            query['mailing_type'] = 'autocast'
            if options['period'] != 'custom':
                self.period = Period.from_def_ranges(options['period'])
                query['start'] = self.period.start
                query['end'] = self.period.end
                query['status'] = 'ok'
            else:
                # WidgetStatForm has only two fields (start, end) which is useful in this case
                form = WidgetStatForm(options)
                if not form.is_valid():
                    return {'status': 'error', 'errors': humanize_form_errors(form)}
                form_data = form.cleaned_data

                start = form_data['start']
                end = form_data['end'].replace(hour=23, minute=59, second=59)

                if start.date() > datetime.datetime.now().date():
                    form.errors['start'] = [_(u"Начало периода не может быть позже текущего дня.")]
                    return {'status': 'error', 'errors': humanize_form_errors(form)}
                if end.date() > datetime.datetime.now().date():
                    form.errors['end'] = [_(u"Конец периода не может быть позже текущего дня.")]
                    return {'status': 'error', 'errors': humanize_form_errors(form)}
                if start > end:
                    form.errors["start"] = [_(u'Начало периода не должно быть больше конца периода.')]
                    return {'status': 'error', 'errors': humanize_form_errors(form)}
                query['start'] = start
                query['end'] = end
                query['status'] = 'ok'
        if options['mailing_type'] == 'campaign':
            query['mailing_type'] = 'campaign'
            query['status'] = 'ok'

        return query

    def get_data(self, options):
        if options['status'] == 'empty':
            self.chart_settings['dataProvider'] = []
            return {'chart_settings': self.chart_settings}
        ids = options['ids']

        if options['mailing_type'] == 'autocast':
            start = options['start']
            end = options['end']

            autocasts = Autocast.objects(id__in=ids)
            messages = [acast.message for acast in autocasts]
            messages = []
            for autocast in autocasts:
                messages.append(DBRef('messages', autocast.message.id))
                if autocast.cases:
                    messages.extend([DBRef('messages', case.message.id) for case in autocast.cases[1:]])

            match_stage = {
                "$match": {
                    "message": {"$in": messages},
                    "time_added": {
                        "$gte": start,
                        "$lte": end
                    }
                }
            }
            emails = list(db.emails.aggregate([match_stage]))
            total_sent = len(emails)
            total_opened = len([email for email in emails if email['opened']])
            total_clicked = len([email for email in emails if email['clicked']])
        else:
            messages = Message.objects(id__in=ids)
            total_sent = Email.objects(message__in=messages).count()
            total_opened = Message.objects(id__in=ids).sum('opens')
            total_clicked = Message.objects(id__in=ids).sum('clicks')

        if not total_sent:
            dataProvider = []
        else:
            opens_desc = '({:.3g}%)'.format(total_opened / total_sent * 100)
            clicks_desc = '({:.3g}%)'.format(total_clicked / total_sent * 100)

            dataProvider = [{
                'title': _(u"Всего писем отправлено"),
                'value': total_sent,
            }, {
                'title': _(u"Всего писем открыто"),
                'value': total_opened,
                'description': opens_desc
            }, {
                'title': _(u"Всего переходов по письмам"),
                'value': total_clicked,
                'description': clicks_desc
            }]

        # These are slices colours in the descending order
        colors = ["#525263", "#337ab7", "#00b359", "#bd3c3c"]
        self.chart_settings["colors"] = colors
        self.chart_settings['dataProvider'] = dataProvider
        return {
            'chart_settings': self.chart_settings
        }


class RecommendationsChart(Chart):
    def __init__(self):
        super(RecommendationsChart, self).__init__()
        self.events = {
            'visits': {'name': 'visits', 'verbose_name':
                    ugettext(u'Кликнуло на виджет рекомендаций'), 'color': '#168de2'},
            'orders': {'name': 'orders', 'verbose_name':
                    ugettext(u'Заказов после клика на виджет рекомендаций'), 'color': '#009ca6'},
            'sum_orders': {'name': 'sum_orders', 'verbose_name':
                    ugettext(u'Сумма заказов после клика на виджет рекомендаций'), 'color': '#555555'},
        }

    def validate_input(self, options):
        recommendations_analytics_startdate = settings.RECOMMENDATIONS_ANALYTICS_ACTIVE_DATE
        if options.get('period') != 'custom':
            options = replace_period_with_dates(options)

        result = validate_time_period(options['period_start'], options['period_end'])
        if result['status'] == 'error':
            return result
        else:
            if result['start'] < recommendations_analytics_startdate:
                result['start'] = recommendations_analytics_startdate
                result['message'] = _(u'Начало периода для данной статистики может быть только с ') + \
                                                            recommendations_analytics_startdate.strftime('%d-%m-%Y')
                if result['end'] < recommendations_analytics_startdate:
                    result['end'] = recommendations_analytics_startdate.replace(hour=23, minute=59, second=59)
            return {
                'status': result['status'],
                'start': result['start'],
                'end': result['end'],
                'message': result.get('message'),
                'widget_option': options['widget_option'],
                'site_id': options['site_id']
            }

    def get_data(self, options):
        self.period.start = options['start']
        self.period.end = options['end']
        data = self.build_query_and_get_data(options)
        graphs_and_axes = self.generate_graphs_and_axes(['visits', 'orders', 'sum_orders'])

        graphs_and_axes['graphs'][0]['type'] = 'column'
        graphs_and_axes['graphs'][0]['fillAlphas'] = '0.7'
        graphs_and_axes['graphs'][0]['hidden'] = True
        graphs_and_axes['graphs'][1]['fillAlphas'] = '0.7'
        graphs_and_axes['graphs'][1]['type'] = 'column'
        graphs_and_axes['axes'][2]['position'] = 'right'
        graphs_and_axes['graphs'][2]['balloon'] = {'drop': False}

        self.chart_settings['valueAxes'] = [graphs_and_axes['axes'][0], graphs_and_axes['axes'][2]]
        self.chart_settings['graphs'] = graphs_and_axes['graphs']
        self.chart_settings['dataProvider'] = data
        self.chart_settings['legend']['fontSize'] = 13

        if options['start'].day != options['end'].day:
            del self.chart_settings['categoryAxis']['minPeriod']

        unique_clicked_visits = sum([item['visits'] for item in data])
        triggered_orders = sum([item['orders'] for item in data])
        sum_triggered_orders = sum([item['sum_orders'] for item in data])
        conversion_rate = (triggered_orders / unique_clicked_visits * 100) if unique_clicked_visits else 0

        return {
            'status': 'ok',
            'chart_settings': self.chart_settings,
            'step': self.period.step,
            'start': self.period.start.strftime('%d-%m-%Y'),
            'end': self.period.end.strftime('%d-%m-%Y'),
            'message': options['message'],
            'unique_clicked_visits': unique_clicked_visits,
            'triggered_orders': triggered_orders,
            'sum_triggered_orders': round(sum_triggered_orders, 2),
            'conversion_rate': round(conversion_rate, 2),

        }

    def build_query_and_get_data(self, options):
        data = []
        if options['widget_option'] != 'all':
            recommend_widgets_ids = [options['widget_option']]
        else:
            recommend_widgets_ids = get_recommendations_widgets(options['site_id']).keys()

        pipeline = [
            {
                '$match': {
                    'time_added': {'$gte': options['start'], '$lte': options['end']},
                    'event_type': 'click', 'init_by': {'$in': recommend_widgets_ids},
                    'visit': {'$exists': 'true'}
                }
            },
            {
                '$group': {
                    '_id': '$visit',
                    'earliest_click_time': {'$min': '$time_added'}
                }
            },
        ]
        req_clicks = list(LeadEvent.objects.aggregate(*pipeline))
        req_visits = {}
        for visit in req_clicks:
            req_visits[visit['_id']] = {'click_time': visit['earliest_click_time']}

        req_orders = LeadOrder.objects(lead_visit__in=req_visits.keys())
        for order in req_orders:
            ref_visit = DBRef('visits', ObjectId(order.lead_visit.id))
            if order.time_added > req_visits[ref_visit]['click_time']:
                req_visits[ref_visit]['completed_order'] = req_visits[ref_visit].get('completed_order', 0) + 1
                if order['cart_sum']:
                    req_visits[ref_visit]['sum_orders'] = req_visits[ref_visit].get('sum_orders', 0) + order['cart_sum']

        agg_func_param = '%Y-%m-%d'
        if self.period.step == 'hour':
            agg_func_param = '%Y-%m-%d %H:00'
        req_visits = sorted(req_visits.values(), key=lambda y: y['click_time'])
        for dt, grp in itertools.groupby(req_visits, key=lambda x: x['click_time'].strftime(agg_func_param)):
            temp = {'date': dt, 'visits': 0, 'orders': 0, 'sum_orders': 0}
            for el in grp:
                temp['visits'] += 1
                temp['orders'] += el.get('completed_order', 0)
                temp['sum_orders'] += float(el.get('sum_orders', 0))
            data.append(temp)
        return data


class EmailDynamicsChart(Chart):
    def __init__(self):
        super(EmailDynamicsChart, self).__init__()
        self.events = {
            'new_multileads': {'name': 'new_multileads', 'verbose_name': ugettext(u'Количество новых клиентов'),
                                                                                                        'color': '#009ca6'},
            'unsubscribed': {'name': 'unsubscribed', 'verbose_name':
                    ugettext(u'Количество клиентов, отписавшихся от всех рассылок'), 'color': '#555555'},
            'coef': {'name': 'coef', 'verbose_name':
                    ugettext(u'Количество отписавшихся клиентов / количество новых'), 'color': '#bd3c3c'},
        }

    def validate_input(self, options):
        if options.get('period') != 'custom':
            options = replace_period_with_dates(options)

        result = validate_time_period(options['period_start'], options['period_end'])
        return result

    def get_data(self, options):
        self.period.start = options['start']
        self.period.end = options['end']
        data, total_new, total_unsubscribed = self.build_query_and_get_data(options)
        graphs_and_axes = self.generate_graphs_and_axes(['new_multileads', 'unsubscribed', 'coef'])

        graphs_and_axes['graphs'][0]['type'] = 'column'
        graphs_and_axes['graphs'][0]['fillAlphas'] = '0.7'
        graphs_and_axes['graphs'][1]['fillAlphas'] = '0.7'
        graphs_and_axes['graphs'][1]['type'] = 'column'
        graphs_and_axes['axes'][2]['position'] = 'right'

        self.chart_settings['valueAxes'] = [graphs_and_axes['axes'][0], graphs_and_axes['axes'][2]]
        self.chart_settings['graphs'] = graphs_and_axes['graphs']
        self.chart_settings['dataProvider'] = data
        self.chart_settings['legend']['fontSize'] = 13

        if options['start'].day != options['end'].day:
            del self.chart_settings['categoryAxis']['minPeriod']
        else:
            self.chart_settings['categoryAxis']['minPeriod'] = 'hh'
        return {
            'status': 'ok',
            'chart_settings': self.chart_settings,
            'step': self.period.step,
            'start': self.period.start.strftime('%d-%m-%Y'),
            'end': self.period.end.strftime('%d-%m-%Y'),
            'message': options.get('message'),
            'new_multileads': total_new,
            'unsubscribed': total_unsubscribed,
            'coef': round(float(total_unsubscribed) / total_new, 3) if total_new else None,
        }

    def build_query_and_get_data(self, options):
        data = []
        unsubscribed_quantity_by_date = {}
        multileads_quantity_by_date = {}
        total_unsubscribed = 0
        total_new = 0

        agg_func_param = '%Y-%m-%d'
        if self.period.step == 'hour':
            agg_func_param = '%Y-%m-%d %H:00'
        pipeline =  [{'$project': {'date': {'$dateToString': {'format': agg_func_param, 'date': '$time_added' } } } },
                     {'$group': {'_id': '$date', 'quantity': {'$sum': 1}}},
                     {'$sort': {'_id': 1}},
                     {'$project': {'date': '$_id', 'quantity': 1, '_id': 0}}]

        unsubscribed = list(UnsubscribedEmail.objects(site=str(options['site_id']), time_added__gte=options['start'],
                                                    time_added__lte=options['end'], status='all').aggregate(*pipeline))

        new_multileads = list(Multilead.objects(site=options['site_id'], time_added__gte=options['start'],
                                                time_added__lte=options['end']).aggregate(*pipeline))

        for x in unsubscribed:
            unsubscribed_quantity_by_date[x['date']] = x['quantity']
        for y in new_multileads:
            multileads_quantity_by_date[y['date']] = y['quantity']

        for dt in sorted(list(set(multileads_quantity_by_date.keys() + unsubscribed_quantity_by_date.keys()))):
            count_unsubscribed = unsubscribed_quantity_by_date.get(dt, 0)
            count_new = multileads_quantity_by_date.get(dt, 0)
            total_new += count_new
            total_unsubscribed += count_unsubscribed
            coef = round(float(count_unsubscribed) / count_new, 3) if count_new else None
            data.append({'date': dt, 'unsubscribed': count_unsubscribed, 'new_multileads': count_new, 'coef': coef})
        return data, total_new, total_unsubscribed


class VisitsChart(Chart):
    def __init__(self):
        super(VisitsChart, self).__init__()
        self.events = {
            'visits': {'name': 'visits', 'verbose_name': ugettext(u'Визиты'), 'color': '#258cbb'},
        }

    def validate_input(self, post_data):
        p = Period()
        options = {'site': post_data['site_id']}
        options['aggr_condition'] = post_data['aggr_condition']

        if post_data.get('initial'):
            options.update({
                'period_start': p.start,
                'period_end': p.end,
                'source': 'all',
                'step': 'day'
            })
            return options

        form = StatForm(post_data)
        if not form.is_valid():
            return {'error': humanize_form_errors(form)}

        form_data = form.cleaned_data
        options['source'] = form_data['source']
        period = form_data['period']

        if period != 'custom':
                p = Period.from_def_ranges(period)
        else:
            if not form_data['period_start']:
                form.errors['period_start'] = [ugettext(u'Введите дату.')]
                return {'error': humanize_form_errors(form), 'status': 'error'}
            if not form_data['period_end']:
                form.errors['period_end'] = [ugettext(u'Введите дату.')]
                return {'error': humanize_form_errors(form), 'status': 'error'}
            if form_data['period_start'].date() > datetime.now().date():
                form.errors['period_start'] = [ugettext(u"Начало периода не может быть позже текущего дня.")]
                return {'error': humanize_form_errors(form), 'status': 'error'}
            if form_data['period_end'].date() > datetime.now().date():
                form.errors['period_end'] = [ugettext(u"Конец периода не может быть позже текущего дня.")]
                return {'error': humanize_form_errors(form), 'status': 'error'}
            if form_data['period_start'] > form_data['period_end']:
                form.errors['period_start'] = [ugettext(u'Начало периода не должно быть больше конца периода.')]
                return {'error': humanize_form_errors(form), 'status': 'error'}

            p.set_start_and_end(form_data['period_start'], form_data['period_end'])

        options.update({'period_end': p.end,
                        'period_start': p.start,
                        'step': p.step,
                        'status': 'ok'
                        })
        return options

    def get_visits_data(self, options):

        match_stage = {
            '$match': {
                'site': DBRef('sites', ObjectId(options['site'])),
                'start': {'$gte': options['period_start']},
                'end': {'$lte': options['period_end']},
            }
        }

        group_stage = {
            '$group': {
                '_id': {'$dateToString': {'format': '%Y-%m-%d %H:00', 'date': '$start'}},
                'total': {'$sum': 1}
            }
        }

        unique_leads_group_stage = {
            '$group': {
                '_id': '$lead',
                'start': {'$first': '$start'}
            }
        }

        if options['source'] == 'rest':
            all_regexes = [{'referrer': {'$regex': regex}} for src in TraffSource.objects() for regex in src.regexp_list if regex]
            match_stage['$match']['$nor'] = all_regexes

        if options['source'] != 'all' and options['source'] != 'rest':
            traff_source = TraffSource.objects(name=options['source']).first()
            if traff_source.kind == 'main':
                regex_query = self.get_regex_query(traff_source)
                if regex_query:
                    match_stage['$match']['$or'] = regex_query
            else:
                regexp_list = [regex.decode('string_escape') for regex in traff_source.regexp_list]
                regex_query = [{'referrer': {'$regex': regex}} for regex in regexp_list]
                match_stage['$match']['$or'] = regex_query

        if options['aggr_condition'] == 'visits':
            visits = list(db.visits.aggregate([match_stage, group_stage]))

        if options['aggr_condition'] == 'leads':
            visits = list(db.visits.aggregate([match_stage, unique_leads_group_stage, group_stage]))

        visits = [{'date': visit['_id'], 'visits': visit['total']} for visit in visits]
        visits = sorted(visits, key=lambda x: x['date'])

        total = sum([i['visits'] for i in visits])
        return {
            'data': {
                'graph': visits,
                'total_per_period': total
            }
        }

    def get_data(self, options):
        graphs_and_axes = self.generate_graphs_and_axes(self.events.keys())
        data = self.get_visits_data(options)
        self.chart_settings['valueAxes'] = graphs_and_axes['axes']
        self.chart_settings['graphs'] = graphs_and_axes['graphs']
        self.chart_settings['dataProvider'] = data['data']['graph']
        self.chart_settings['marginLeft'] = 20
        self.chart_settings['marginRight'] = 20
        self.chart_settings['legend'] = None

        return {'chart_settings': self.chart_settings, 'total_visits': data['data']['total_per_period']}

    def get_regex_query(self, traff_source):
        sources_ids = [ObjectId(_id) for _id in traff_source.sources_list]
        sources = TraffSource.objects(id__in=sources_ids)
        regexp_list = []
        for source in sources:
            for regex in source.regexp_list:
                regexp_list.append(regex.decode('string_escape'))

        regex_query = []
        for regex in regexp_list:
            regex_query.append({'referrer': {'$regex': regex}})
        return regex_query


class SalesFunnelChart(FunnelChart):
    def __init__(self, site_id=None):
        super(SalesFunnelChart, self).__init__()
        self.store_managers = list(Lead.objects(site=site_id, status='manager').values_list('id'))

    def validate_input(self, options):
        options = copy.deepcopy(options)
        result = validate_date_range(options)
        options.update(result)

        return options

    def get_conversion(self, x, y):
        if not y:
            return ': 0'
        return ': {}%'.format(round(x / y * 100, 2))

    def get_data(self, options):

        site_id = ObjectId(options['site_id'])
        site_ref = DBRef('sites', site_id)
        start = options['start']
        end = options['end']

        visited_pages_query = {
            'site': site_ref,
            'date': {
                '$gte': start,
                '$lte': end
            },
        }

        norm_visits = list(db.visits.find({
            'site': site_ref,
            'start': {'$gte': start},
            'end': {'$lte': end}
        }))

        norm_visits_leads = set([visit['lead'] for visit in norm_visits])

        incognito_visits = list(db.incognito_pageviews.find(visited_pages_query))
        lead_visits = list(db.lead_visited_pages.find(visited_pages_query))

        unique_incognito = set((visit['lead'] for visit in incognito_visits))
        unique_leads = set((visit['lead'] for visit in lead_visits))

        all_visitors = list(unique_incognito)
        all_visitors.extend(unique_leads)

        try:
            yml_file = YMLFile.objects.get(site=site_id)
            offers_hashes = set([offer.url_hash for offer in YMLOffer.objects(site=site_id)])
        except YMLFile.DoesNotExist:
            yml_file = YMLFile()
            offers_hashes = []

        leads_visited_offers = set()
        for visit in incognito_visits:
            url_hash = hashlib.md5(get_minimal_url(visit['page'], yml_file.significant_params, regex=yml_file.offer_regex)).hexdigest()[:12]
            if url_hash in offers_hashes:
                leads_visited_offers.add(visit['lead'])

        for visit in lead_visits:
            url_hash = hashlib.md5(get_minimal_url(visit['page'], yml_file.significant_params, regex=yml_file.offer_regex)).hexdigest()[:12]
            if url_hash in offers_hashes:
                leads_visited_offers.add(visit['lead'])

        cart_items = db.cart_items.find({
            'site': site_ref,
            'lead': {'$in': all_visitors},
            'time_added': {
                '$gte': start,
                '$lte': end
            }
        })

        unique_leads_added_to_cart = set((cart_item['lead'] for cart_item in cart_items))
        orders = db.lead_orders.find({
            'site': site_ref,
            'lead.$id': {'$nin': self.store_managers},
            # 'lead': {'$in': all_visitors},
            'time_added': {
                '$gte': start,
                '$lte': end
            }
        })

        unique_leads_made_orders = set((order['lead'] for order in orders))

        total_visits = len(norm_visits_leads)
        total_offer_views = len(leads_visited_offers)
        total_additions_to_cart = len(unique_leads_added_to_cart)
        total_orders_made = len(unique_leads_made_orders)
        conversion_msg = ugettext(u'Конверсия')
        abs_conversion_msg = ugettext(u'Абсолютная конверсия')

        self.chart_settings['dataProvider'] = [
            {
                'title': ugettext(u'Посетило сайт'),
                'real_value': total_visits,
                'value': 10
            },
            {
                'title': ugettext(u'Страницы товаров посмотрело'),
                'real_value': total_offer_views,
                'value': 9,
                'description': conversion_msg + self.get_conversion(total_offer_views, total_visits)

            },
            {
                'title': ugettext(u'Добавило в корзину'),
                'real_value': total_additions_to_cart,
                'value': 8,
                'description': conversion_msg + self.get_conversion(total_additions_to_cart, total_offer_views)
            }, {
                'title': ugettext(u'Сделало заказ'),
                'real_value': total_orders_made,
                'value': 7,
                'description': conversion_msg + self.get_conversion(total_orders_made, total_additions_to_cart) \
                + '\n' + abs_conversion_msg + self.get_conversion(total_orders_made, total_visits)
            }
        ]
        self.chart_settings['balloonText'] = '[[title]]: [[real_value]]\n[[description]]'
        self.chart_settings['labelText'] = '[[title]]: [[real_value]]'
        self.chart_settings['colors'] = ['#ff4b00', '#15bbff', '#ffd800', '#49dc3a']
        self.chart_settings['neckWidth'] = 30
        del self.chart_settings['depth3D']
        del self.chart_settings['angle']

        carts_abandoned = total_additions_to_cart - total_orders_made
        offers_views_lost = total_offer_views - total_additions_to_cart
        no_offers_viewed = total_visits - total_offer_views

        if not total_visits:
            self.chart_settings['dataProvider'] = []
            carts_abandoned = 0
            offers_views_lost = 0
            no_offers_viewed = 0

        return {
            'chart_settings': self.chart_settings,
            'carts_abandoned': carts_abandoned,
            'offers_views_lost': offers_views_lost,
            'no_offers_viewed': no_offers_viewed
        }


class SalesBarChart(Chart):
    def __init__(self, site_id=None):
        super(SalesBarChart, self).__init__()
        self.store_managers = list(Lead.objects(site=site_id, status='manager').values_list('id'))

    def validate_input(self, options):
        options = copy.deepcopy(options)
        options['status'] = 'ok'
        return options

    def get_rec_widgets_based_orders(self, options):

        period = Period.from_def_ranges(options['period'])
        rec_widgets_ids = get_recommendations_widgets(options['site_id']).keys()
        pipeline = [
            {
                '$match': {
                    'time_added': {'$gte': period.start, '$lte': period.end},
                    'event_type': 'click',
                    'init_by': {'$in': rec_widgets_ids},
                    'visit': {'$exists': 'true'},
                    'lead.$id': {'$nin': self.store_managers}
                }
            },
            {
                '$group': {
                    '_id': '$visit',
                    'earliest_click_time': {'$min': '$time_added'}
                }
            },
        ]

        events = {visit['_id']: visit['earliest_click_time'] for visit in db.lead_events.aggregate(pipeline)}
        orders = LeadOrder.objects(lead_visit__in=events.keys())

        result = []
        for order in orders:
            ref_visit = DBRef('visits', ObjectId(order.lead_visit.id))
            if order.time_added > events[ref_visit]:
                result.append(order.id)
        return result

    def get_data(self, options):
        rec_orders = self.get_rec_widgets_based_orders(options)

        period = Period.from_def_ranges(options['period'])
        query = Q(site=options['site_id'], time_added__gte=period.start, time_added__lte=period.end, lead__nin=self.store_managers)
        query &= (Q(mass_email__exists=True) | Q(trigger_email__exists=True))
        email_orders = LeadOrder.objects(query).values_list('id')

        leadhit_based_orders = set(rec_orders)
        leadhit_based_orders.update(set(email_orders))

        all_orders = LeadOrder.objects(site=options['site_id'], time_added__gte=period.start, time_added__lte=period.end, lead__nin=self.store_managers)
        all_orders = {order.id: {'time_added': order.time_added, 'cart_sum': order.cart_sum} for order in all_orders}

        store_made_orders_ids = set(all_orders.keys())
        store_made_orders_ids.difference_update(leadhit_based_orders)

        orders_dict = {}

        agg_func_param = '%Y-%m-%d'
        if options['period'] == 'day':
            agg_func_param = '%Y-%m-%d %H:00'

        for order in all_orders:
            date = all_orders[order]['time_added'].strftime(agg_func_param)
            cart_sum = float(all_orders[order]['cart_sum'] or 0)

            if date not in orders_dict:
                orders_dict[date] = {}
                if order in store_made_orders_ids:
                    orders_dict[date]['store'] = cart_sum
                else:
                    orders_dict[date]['leadhit'] = cart_sum
            else:
                if order in store_made_orders_ids:
                    if 'store' not in orders_dict[date]:
                        orders_dict[date]['store'] = cart_sum
                    else:
                        orders_dict[date]['store'] += cart_sum
                else:
                    if 'leadhit' not in orders_dict[date]:
                        orders_dict[date]['leadhit'] = cart_sum
                    else:
                        orders_dict[date]['leadhit'] += cart_sum

        dataProvider = []
        for date in orders_dict:
            tmp_obj = {
                'date': date,
            }

            if 'store' in orders_dict[date]:
                tmp_obj['store'] = orders_dict[date]['store']

            if 'store' in orders_dict[date] and 'leadhit' in orders_dict[date]:
                tmp_obj['leadhit'] = orders_dict[date]['store'] + orders_dict[date]['leadhit']
                tmp_obj['raw_leadhit'] = orders_dict[date]['leadhit']

            if 'store' not in orders_dict[date] and 'leadhit' in orders_dict[date]:
                tmp_obj['leadhit'] = orders_dict[date]['leadhit']
                tmp_obj['raw_leadhit'] = orders_dict[date]['leadhit']

            dataProvider.append(tmp_obj)

        dataProvider.sort(key=lambda x: x['date'])

        self.chart_settings['dataProvider'] = dataProvider
        self.chart_settings['valueAxes'] = [{'position': 'left'}]

        if options['period'] != 'day':
            del self.chart_settings['categoryAxis']['minPeriod']
        else:
            self.chart_settings['categoryAxis']['minPeriod'] = 'hh'
        self.chart_settings['chartCursor']['cursorColor'] = '#6ba7ba'

        income_msg = ugettext(u'Сумма заказов')
        leadhit_income_msg = ugettext(u'Сумма заказов с')

        self.chart_settings['graphs'] = [
            {
                'balloonText': income_msg + ': [[store]]',
                'fillAlphas': 0.9,
                'lineAlpha': 0.2,
                'title': income_msg,
                'type': 'column',
                'valueField': 'store',
                'fillColors': '#7c8387'
            },
            {
                'balloonText': leadhit_income_msg + ' <b>LeadHit</b>: [[leadhit]]',
                'fillAlphas': 0.95,
                'lineAlpha': 0.2,
                'title': leadhit_income_msg + ' LeadHit',
                'type': 'column',
                'clustered': False,
                'columnWidth': 0.5,
                'valueField': 'leadhit',
                'fillColors': '#fbe916',
            }
        ]

        store_orders_count = len(store_made_orders_ids)
        store_orders_sum = sum([item['store'] for item in dataProvider if item.get('store')])
        leadhit_orders_count = len(leadhit_based_orders)
        leadhit_orders_sum = sum([item['raw_leadhit'] for item in dataProvider if item.get('raw_leadhit')])
        all_orders_count = len(all_orders)
        all_orders_sum = leadhit_orders_sum + store_orders_sum
        return {
            'chart_settings': self.chart_settings,
            'store_orders_count': store_orders_count,
            'store_orders_sum': store_orders_sum,
            'leadhit_orders_count': leadhit_orders_count,
            'leadhit_orders_sum': leadhit_orders_sum,
            'all_orders_count': all_orders_count,
            'all_orders_sum': all_orders_sum,
        }


class LeadsDiscoveryChart(Chart):

    def __init__(self):
        super(LeadsDiscoveryChart, self).__init__()
        self.events = {
            'store': {'name': 'store', 'verbose_name': ugettext(u'Лидов найдено'), 'color': 'wheat'},
            'leadhit': {'name': 'leadhit', 'verbose_name': ugettext(u'Лидов найдено с'), 'color': 'yellow'}
        }

    def validate_input(self, options):
        options = copy.deepcopy(options)
        options['status'] = 'ok'
        return options

    def get_lh_forms(self, site):

        LH_FORM_ACTIONS = ('lh_dp', 'lh_banner', 'lh_banner_mobile')

        query = {
            'site': site,
            '$or': [
                {'action': {'$in': LH_FORM_ACTIONS}},
                {'subscription': True}
            ]
        }

        site_forms = db.forms.find(query)
        site_forms = set([DBRef('forms', form['_id']) for form in site_forms])

        return site_forms

    def get_leads_ids_and_refs(self, site, period):

        multi = list(db.multileads.find({
            'site': site,
            'time_added': {
                '$gte': period.start,
                '$lte': period.end
            }
        }, {'_id': 1}))

        leads = list(db.leads.find({
            'site': site,
            'multilead': {'$in': [DBRef('multileads', m['_id']) for m in multi]},
            'time_added': {
                '$gte': period.start,
                '$lte': period.end
            }
        }, {'_id': 1, 'multilead': 1}))

        leads_ids = [lead['_id'] for lead in leads]
        leads_refs = [DBRef('leads', lead) for lead in leads_ids]
        leads_multileads_dict = {DBRef('leads', lead['_id']): lead['multilead'] for lead in leads}

        return {
            'leads_ids': leads_ids,
            'leads_refs': leads_refs,
            'leads_multileads_dict': leads_multileads_dict
        }

    def get_filled_forms(self, site, leads_refs):

        match_stage = {
            '$match': {
                'site': site,
                'lead': {'$in': leads_refs}
            }
        }

        group_stage = {
            '$group': {
                '_id': '$lead',
                'forms': {'$push': {'form_dbref': '$leadform', 'submitted_time': '$submitted_time'}}
            }
        }

        unwind_stage = {
            '$unwind': '$forms'
        }

        sort_stage = {
            '$sort': {
                'forms': 1
            }
        }

        second_group_stage = {
            '$group': {
                '_id': '$_id',
                'filled_forms': {'$push': '$forms'}
            }
        }

        pipeline = [match_stage, group_stage, unwind_stage, sort_stage, second_group_stage]
        # forms is a list of objects where '_id' is lead_id and 'filled_forms' is a sorted list of forms that given lead filled
        forms = list(db.leads_filled_forms.aggregate(pipeline))
        return forms

    def get_data(self, options):

        period = Period.from_def_ranges(options['period'])
        site = DBRef('sites', ObjectId(options['site_id']))
        options['site'] = site

        agg_func_param = '%Y-%m-%d'
        if period.step == 'hour':
            self.chart_settings['categoryAxis']['minPeriod'] = 'hh'
            agg_func_param = '%Y-%m-%d %H:00'
        else:
            del self.chart_settings['categoryAxis']['minPeriod']

        relevant_site_forms = self.get_lh_forms(site)
        leads = self.get_leads_ids_and_refs(site, period)
        forms = self.get_filled_forms(site, leads['leads_refs'])

        leads_multileads_dict = leads['leads_multileads_dict']
        processed_multileads = set()
        data = {}

        for lead in forms:
            multilead_dbref = leads_multileads_dict[lead['_id']]
            first_form = lead['filled_forms'][0]

            # no multilead case
            if not multilead_dbref:
                pass

            if multilead_dbref in processed_multileads:
                continue

            processed_multileads.add(multilead_dbref)
            form_submission_date = first_form['submitted_time'].strftime(agg_func_param)
            leadhit = first_form['form_dbref'] in relevant_site_forms
            if form_submission_date not in data:
                data[form_submission_date] = {}
                if not leadhit:
                    data[form_submission_date]['leadhit'] = 0
                    data[form_submission_date]['store'] = 1
                else:
                    data[form_submission_date]['leadhit'] = 1
                    data[form_submission_date]['store'] = 0
            else:
                if not leadhit:
                    data[form_submission_date]['store'] += 1
                else:
                    data[form_submission_date]['leadhit'] += 1

        processed_data = []
        for date in data:
            store = data[date].get('store', 0)
            leadhit = data[date].get('leadhit', 0)
            obj = {'date': date}
            if store:
                obj['store'] = store
            if store and leadhit:
                obj['leadhit'] = leadhit + store
                obj['raw_leadhit'] = leadhit
            if not store and leadhit:
                obj['raw_leadhit'] = leadhit
                obj['leadhit'] = leadhit
            processed_data.append(obj)
        processed_data.sort(key=lambda x: x['date'])

        self.chart_settings['dataProvider'] = processed_data
        self.chart_settings['valueAxes'] = [{'position': 'left'}]
        self.chart_settings['chartCursor']['cursorColor'] = '#6ba7ba'

        income_msg = ugettext(u'Лидов найдено')
        leadhit_income_msg = ugettext(u'Лидов найдено с')

        self.chart_settings['graphs'] = [
            {
                'balloonText': income_msg + ': [[store]]',
                'fillAlphas': 0.9,
                'lineAlpha': 0.2,
                'title': income_msg,
                'type': 'column',
                'valueField': 'store',
                'fillColors': '#7c8387'
            },
            {
                'balloonText': leadhit_income_msg + ' <b>LeadHit</b>: [[leadhit]]',
                'fillAlphas': 0.95,
                'lineAlpha': 0.2,
                'title': leadhit_income_msg + ' LeadHit',
                'type': 'column',
                'clustered': False,
                'columnWidth': 0.5,
                'valueField': 'leadhit',
                'fillColors': '#fb8c9d',
            }
        ]
        store_leads = sum([date['store'] for date in processed_data if date.get('store')])
        leadhit_leads = sum([date['raw_leadhit'] for date in processed_data if date.get('raw_leadhit')])
        if not (leadhit_leads + store_leads):
            percentage = ' 0%'
        else:
            percentage = ' {}%'.format((round(leadhit_leads / store_leads * 100, 2)))

        return {
            'chart_settings': self.chart_settings,
            'store_leads': store_leads,
            'leadhit_leads': leadhit_leads,
            'leads_diff': percentage
        }


def validate_date_range(options):
    result = {
        'status': 'ok'
    }

    if options['period'] != 'custom':
        period = Period.from_def_ranges(options['period'])
        result['start'] = period.start
        result['end'] = period.end
    else:
        form = WidgetStatForm(options)
        if not form.is_valid():
            result['status'] = 'error'
            result['errors'] = humanize_form_errors(form)
        else:
            form_data = form.cleaned_data

            start = form_data['start']
            end = form_data['end'].replace(hour=23, minute=59, second=59)

            if start.date() > datetime.datetime.now().date():
                form.errors['start'] = [_(u"Начало периода не может быть позже текущего дня.")]
                result['status'] = 'error'
                result['errors'] = humanize_form_errors(form)

            if end.date() > datetime.datetime.now().date():
                form.errors['end'] = [_(u"Конец периода не может быть позже текущего дня.")]
                result['status'] = 'error'
                result['errors'] = humanize_form_errors(form)

            if start > end:
                form.errors["start"] = [_(u'Начало периода не должно быть больше конца периода.')]
                result['status'] = 'error'
                result['errors'] = humanize_form_errors(form)

            result['start'] = start
            result['end'] = end

    return result


class AverageRevenuePerVisitorChart(Chart):
    def __init__(self):
        super(AverageRevenuePerVisitorChart, self).__init__()
        self.events = {
            'arpv': {'name': 'arpv', 'verbose_name': 'ARPV', 'color': '#258cbb'},
        }

    def get_arpv_data(self, options):
        data_by_weeks = get_sum_orders_by_week(
            options['period_start'], options['period_end'], options['site_id']
        )

        data = []
        for isoweek, values in sorted(data_by_weeks.items(), key=lambda x: (x[0], x[1])):
            match_stage_current_week = {
                '$match': {
                    'site': DBRef('sites', options['site_id']),
                    'start': {
                        '$gte': datetime.datetime.combine(values['start'], datetime.datetime.min.time()),
                        '$lte': datetime.datetime.combine(values['end'], datetime.datetime.max.time())
                    }
                }
            }
            group_unique_leads_stage = {
                '$group': {
                    '_id': '$lead',
                    'start': {'$first': '$start'}
                }
            }

            visitors = len(
                list(db.visits.aggregate([match_stage_current_week, group_unique_leads_stage]))
            )
            data.append({
                'date': values['start'].strftime('%d.%m') + '-' + values['end'].strftime('%d.%m'),
                'arpv': round(
                    float(values.get('revenue', 0)) / visitors, 2
                ) if visitors else 0
            })

        return data

    def get_data(self, options):
        graphs_and_axes = self.generate_graphs_and_axes(self.events.keys())
        data = self.get_arpv_data(options)
        self.chart_settings['valueAxes'] = graphs_and_axes['axes']
        self.chart_settings['graphs'] = graphs_and_axes['graphs']
        self.chart_settings['dataProvider'] = data
        self.chart_settings['legend'] = None
        self.chart_settings['categoryAxis']['parseDates'] = False
        return {'chart_settings': self.chart_settings}


class AverageRevenuePerUserChart(Chart):
    def __init__(self):
        super(AverageRevenuePerUserChart, self).__init__()
        self.events = {
            'arpu': {'name': 'arpu', 'verbose_name': 'ARPU', 'color': '#258cbb'},
        }

    def get_arpu_data(self, options):
        data_by_weeks = get_sum_orders_by_week(
            options['period_start'], options['period_end'], options['site_id']
        )
        data = []

        for isoweek, values in sorted(data_by_weeks.items(), key=lambda x: (x[0], x[1])):
            current_week_visitors = db.visits.find({
                "start": {
                    '$gte': datetime.datetime.combine(values['start'], datetime.datetime.min.time()),
                    '$lte': datetime.datetime.combine(values['end'], datetime.datetime.max.time())
                },
                "site": DBRef('sites', options['site_id'])
            }).distinct('lead')

            current_week_multileads = len(db.leads.find({
                "_id": {'$in': [lead.id for lead in current_week_visitors]},
            }).distinct('multilead'))

            data.append({
                'date': values['start'].strftime('%d.%m') + '-' + values['end'].strftime('%d.%m'),
                'arpu': round(
                    float(values.get('revenue', 0)) / current_week_multileads, 2
                ) if current_week_multileads else 0
            })

        return data

    def get_data(self, options):
        graphs_and_axes = self.generate_graphs_and_axes(self.events.keys())
        data = self.get_arpu_data(options)
        self.chart_settings['valueAxes'] = graphs_and_axes['axes']
        self.chart_settings['graphs'] = graphs_and_axes['graphs']
        self.chart_settings['dataProvider'] = data
        self.chart_settings['legend'] = None
        self.chart_settings['categoryAxis']['parseDates'] = False
        return {'chart_settings': self.chart_settings}


class AverageRevenuePerPayingUserChart(Chart):
    def __init__(self):
        super(AverageRevenuePerPayingUserChart, self).__init__()
        self.events = {
            'arppu': {'name': 'arppu', 'verbose_name': 'ARPPU', 'color': '#258cbb'},
        }

    def get_arppu_data(self, options):
        period_start = options['period_start']
        period_end = options['period_end']

        date_list = [period_start.date() + datetime.timedelta(days=x) for x
                       in xrange((period_end - period_start).days + 1)]
        data_by_weeks = {}
        for id_grp, grp in itertools.groupby(date_list, key=lambda x: x.isocalendar()[:2]):
            current_week = list(grp)
            data_by_weeks[id_grp] = {'start': min(current_week), 'end': max(current_week)}

        data = []
        for isoweek, values in sorted(data_by_weeks.items(), key=lambda x: (x[0], x[1])):
            week_orders = LeadOrder.objects(
                site=DBRef('sites', options['site_id']),
                time_added__gte=datetime.datetime.combine(values['start'], datetime.datetime.min.time()),
                time_added__lte=datetime.datetime.combine(values['end'], datetime.datetime.max.time())
            )

            solvent_leads = week_orders.no_dereference().values_list('lead')
            solvent_multileads = Lead.objects(
                id__in=[x.id for x in solvent_leads]
            ).no_dereference().scalar('multilead', 'id')
            solvent_multileads = {x[0] if x[0] else x[1] for x in solvent_multileads}
            week_unique_customers = len(solvent_multileads)

            data.append({
                'date': values['start'].strftime('%d.%m') + '-' + values['end'].strftime('%d.%m'),
                'arppu': round(
                    float(week_orders.sum('cart_sum')) / week_unique_customers, 2
                ) if week_unique_customers else 0
            })

        return data

    def get_data(self, options):
        graphs_and_axes = self.generate_graphs_and_axes(self.events.keys())
        data = self.get_arppu_data(options)
        self.chart_settings['valueAxes'] = graphs_and_axes['axes']
        self.chart_settings['graphs'] = graphs_and_axes['graphs']
        self.chart_settings['dataProvider'] = data
        self.chart_settings['legend'] = None
        self.chart_settings['categoryAxis']['parseDates'] = False
        return {'chart_settings': self.chart_settings}


class CartAbandonmentRateChart(Chart):
    def __init__(self):
        super(CartAbandonmentRateChart, self).__init__()
        self.events = {
            'car': {'name': 'car', 'verbose_name': 'Cart abandonment rate', 'color': '#258cbb'},
        }

    def get_car_data(self, options):
        period_start = options['period_start']
        period_end = options['period_end']

        date_list = [period_start.date() + datetime.timedelta(days=x) for x
                     in xrange((period_end - period_start).days + 1)]
        data_by_weeks = {}
        for id_grp, grp in itertools.groupby(date_list, key=lambda x: x.isocalendar()[:2]):
            current_week = list(grp)
            data_by_weeks[id_grp] = {'start': min(current_week), 'end': max(current_week)}

        data = []
        for isoweek, values in sorted(data_by_weeks.items(), key=lambda x: (x[0], x[1])):
            week_orders_number = LeadOrder.objects(
                site=DBRef('sites', options['site_id']),
                time_added__gte=datetime.datetime.combine(values['start'], datetime.datetime.min.time()),
                time_added__lte=datetime.datetime.combine(values['end'], datetime.datetime.max.time())
            ).count()

            week_baskets_number = len(CartItem.objects(
                site=DBRef('sites', options['site_id']),
                time_added__gte=datetime.datetime.combine(values['start'], datetime.datetime.min.time()),
                time_added__lte=datetime.datetime.combine(values['end'], datetime.datetime.max.time()),
                order_id__exists=True
            ).distinct('order_id'))

            data.append({
                'date': values['start'].strftime('%d.%m') + '-' + values['end'].strftime('%d.%m'),
                'car': round(
                    float(week_baskets_number - week_orders_number) / week_baskets_number * 100, 2
                ) if week_orders_number else 0
            })

        return data

    def get_data(self, options):
        graphs_and_axes = self.generate_graphs_and_axes(self.events.keys())
        data = self.get_car_data(options)
        self.chart_settings['valueAxes'] = graphs_and_axes['axes']
        self.chart_settings['graphs'] = graphs_and_axes['graphs']
        self.chart_settings['dataProvider'] = data
        self.chart_settings['legend'] = None
        self.chart_settings['categoryAxis']['parseDates'] = False
        return {'chart_settings': self.chart_settings}


class AverageCheckChart(Chart):
    def __init__(self):
        super(AverageCheckChart, self).__init__()
        self.events = {
            'average_check': {'name': 'average_check', 'verbose_name': 'Average check', 'color': '#258cbb'},
        }

    def get_graph_data(self, options):
        period_start = options['period_start']
        period_end = options['period_end']
        date_list = [period_start.date() + datetime.timedelta(days=x) for x
                     in xrange((period_end - period_start).days + 1)]
        data_by_weeks = {}

        for id_grp, grp in itertools.groupby(date_list, key=lambda x: x.isocalendar()[:2]):
            current_week = list(grp)
            data_by_weeks[id_grp] = {'start': min(current_week), 'end': max(current_week)}

        data = []
        for isoweek, values in sorted(data_by_weeks.items(), key=lambda x: (x[0], x[1])):
            week_orders = LeadOrder.objects(
                site=DBRef('sites', options['site_id']),
                time_added__gte=datetime.datetime.combine(values['start'], datetime.datetime.min.time()),
                time_added__lte=datetime.datetime.combine(values['end'], datetime.datetime.max.time())
            )
            week_orders_number = week_orders.count()
            data.append({
                'date': values['start'].strftime('%d.%m') + '-' + values['end'].strftime('%d.%m'),
                'average_check': round(
                    float(week_orders.sum('cart_sum')) / week_orders_number, 2
                ) if week_orders_number else 0
            })

        return data

    def get_data(self, options):
        graphs_and_axes = self.generate_graphs_and_axes(self.events.keys())
        data = self.get_graph_data(options)
        self.chart_settings['valueAxes'] = graphs_and_axes['axes']
        self.chart_settings['graphs'] = graphs_and_axes['graphs']
        self.chart_settings['dataProvider'] = data
        self.chart_settings['legend'] = None
        self.chart_settings['categoryAxis']['parseDates'] = False
        return {'chart_settings': self.chart_settings}


class PurchaseFrequencyChart(Chart):
    def __init__(self):
        super(PurchaseFrequencyChart, self).__init__()
        self.events = {
            'purchase_frequency': {'name': 'purchase_frequency', 'verbose_name': 'Purchase frequency', 'color': '#258cbb'},
        }

    def get_graph_data(self, options):
        period_start = options['period_start']
        period_end = options['period_end']
        date_list = [period_start.date() + datetime.timedelta(days=x) for x
                     in xrange((period_end - period_start).days + 1)]
        data_by_weeks = {}

        for id_grp, grp in itertools.groupby(date_list, key=lambda x: x.isocalendar()[:2]):
            current_week = list(grp)
            data_by_weeks[id_grp] = {'start': min(current_week), 'end': max(current_week)}

        data = []
        for isoweek, values in sorted(data_by_weeks.items(), key=lambda x: (x[0], x[1])):
            week_orders = LeadOrder.objects(
                site=DBRef('sites', options['site_id']),
                time_added__gte=datetime.datetime.combine(values['start'], datetime.datetime.min.time()),
                time_added__lte=datetime.datetime.combine(values['end'], datetime.datetime.max.time())
            )

            solvent_leads = week_orders.no_dereference().values_list('lead')
            solvent_multileads = Lead.objects(
                id__in=[x.id for x in solvent_leads]
            ).no_dereference().scalar('multilead', 'id')
            solvent_multileads = {x[0] if x[0] else x[1] for x in solvent_multileads}
            week_unique_customers = len(solvent_multileads)
            week_orders_number = week_orders.count()

            data.append({
                'date': values['start'].strftime('%d.%m') + '-' + values['end'].strftime('%d.%m'),
                'purchase_frequency': round(
                    float(week_orders_number) / week_unique_customers, 2
                ) if week_unique_customers else 0
            })

        return data

    def get_data(self, options):
        graphs_and_axes = self.generate_graphs_and_axes(self.events.keys())
        data = self.get_graph_data(options)
        self.chart_settings['valueAxes'] = graphs_and_axes['axes']
        self.chart_settings['graphs'] = graphs_and_axes['graphs']
        self.chart_settings['dataProvider'] = data
        self.chart_settings['legend'] = None
        self.chart_settings['categoryAxis']['parseDates'] = False
        return {'chart_settings': self.chart_settings}


class PaidOrdersRateChart(Chart):
    def __init__(self):
        super(PaidOrdersRateChart, self).__init__()
        self.events = {
            'paid_orders_rate': {'name': 'paid_orders_rate', 'verbose_name': 'Paid orders rate', 'color': '#258cbb'},
        }

    def get_graph_data(self, options):
        period_start = options['period_start']
        period_end = options['period_end']
        date_list = [period_start.date() + datetime.timedelta(days=x) for x
                     in xrange((period_end - period_start).days + 1)]
        data_by_weeks = {}

        for id_grp, grp in itertools.groupby(date_list, key=lambda x: x.isocalendar()[:2]):
            current_week = list(grp)
            data_by_weeks[id_grp] = {'start': min(current_week), 'end': max(current_week)}

        data = []
        for isoweek, values in sorted(data_by_weeks.items(), key=lambda x: (x[0], x[1])):
            week_orders = LeadOrder.objects(
                site=DBRef('sites', options['site_id']),
                time_added__gte=datetime.datetime.combine(values['start'], datetime.datetime.min.time()),
                time_added__lte=datetime.datetime.combine(values['end'], datetime.datetime.max.time())
            )
            week_orders_number = week_orders.count()

            data.append({
                'date': values['start'].strftime('%d.%m') + '-' + values['end'].strftime('%d.%m'),
                'paid_orders_rate': round(
                    float(week_orders(status='paid').count()) / week_orders_number * 100, 2
                ) if week_orders_number else 0
            })

        return data

    def get_data(self, options):
        graphs_and_axes = self.generate_graphs_and_axes(self.events.keys())
        data = self.get_graph_data(options)
        self.chart_settings['valueAxes'] = graphs_and_axes['axes']
        self.chart_settings['graphs'] = graphs_and_axes['graphs']
        self.chart_settings['dataProvider'] = data
        self.chart_settings['legend'] = None
        self.chart_settings['categoryAxis']['parseDates'] = False
        return {'chart_settings': self.chart_settings}


class RepeatCustomerRateChart(Chart):
    def __init__(self):
        super(RepeatCustomerRateChart, self).__init__()
        self.events = {
            'rcr': {'name': 'rcr', 'verbose_name': 'Repeat customer rate', 'color': '#258cbb'},
        }

    def get_rcr_data(self, options):
        period_start = options['period_start']
        period_end = options['period_end']
        date_list = [period_start.date() + datetime.timedelta(days=x) for x
                       in xrange((period_end - period_start).days + 1)]
        data_by_weeks = {}

        for id_grp, grp in itertools.groupby(date_list, key=lambda x: x.isocalendar()[:2]):
            current_week = list(grp)
            data_by_weeks[id_grp] = {'start': min(current_week), 'end': max(current_week)}

        data = []
        for isoweek, values in sorted(data_by_weeks.items(), key=lambda x: (x[0], x[1])):
            solvent_leads = LeadOrder.objects(
                site=DBRef('sites', options['site_id']),
                time_added__gte=datetime.datetime.combine(values['start'], datetime.datetime.min.time()),
                time_added__lte=datetime.datetime.combine(values['end'], datetime.datetime.max.time())
            ).no_dereference().values_list('lead')
            solvent_multileads = Lead.objects(
                id__in=[x.id for x in solvent_leads]
            ).no_dereference().values_list('multilead', 'id')
            solvent_multileads = {x[1]: x[0] if x[0] else x[1] for x in solvent_multileads}

            solvent_multileads_counter = {}
            for lead in solvent_leads:
                multilead = solvent_multileads[lead.id]
                solvent_multileads_counter[multilead] = solvent_multileads_counter.get(multilead, 0) + 1

            total_solvent_multileads = len(solvent_multileads_counter)
            total_repeat_multileads = len(
                [value for value in solvent_multileads_counter.values() if value > 1]
            )

            data.append({
                'date': values['start'].strftime('%d.%m') + '-' + values['end'].strftime('%d.%m'),
                'rcr': round(
                    float(total_repeat_multileads) / total_solvent_multileads * 100, 2
                ) if total_solvent_multileads else 0
            })

        return data

    def get_data(self, options):
        graphs_and_axes = self.generate_graphs_and_axes(self.events.keys())
        data = self.get_rcr_data(options)
        self.chart_settings['valueAxes'] = graphs_and_axes['axes']
        self.chart_settings['graphs'] = graphs_and_axes['graphs']
        self.chart_settings['dataProvider'] = data
        self.chart_settings['legend'] = None
        self.chart_settings['categoryAxis']['parseDates'] = False
        return {'chart_settings': self.chart_settings}


def replace_period_with_dates(options):
    if options.get('period'):
        options = copy.deepcopy(options)
        period = Period.from_def_ranges(options['period'])
        options['period_start'] = period.start.strftime('%d-%m-%Y')
        options['period_end'] = period.end.strftime('%d-%m-%Y')

    return options