# coding: utf-8
import json
from copy import deepcopy
from datetime import datetime, timedelta

from django.conf import settings
from django.http import JsonResponse
from django.urls import reverse
from django.views.generic import View, TemplateView
from django.utils.decorators import method_decorator
from django.utils.translation import ugettext, get_language

from bson import DBRef
from bson.objectid import ObjectId

from automailer.models import Autocast
from accounts.models import WidgetsConf
from emails.models import Message
from lib.core.acl import role_required, permission_required
from lib.mongoengine_utils import Pagination

from forms import StatForm
from models import TraffSource
from utils.helpers import (
    Period as P,
    process_autocasts_query_timeframe,
    humanize_form_errors,
    validate_time_period,
    get_messages_and_msg_to_autocast_dict
)
from .chart import (
    WidgetsChart,
    LeadsChart,
    EmailCampaignsChart,
    RecommendationsChart,
    EmailDynamicsChart,
    VisitsChart,
    SalesFunnelChart,
    SalesBarChart,
    LeadsDiscoveryChart,
    AverageRevenuePerVisitorChart,
    AverageRevenuePerUserChart,
    AverageRevenuePerPayingUserChart,
    CartAbandonmentRateChart,
    AverageCheckChart,
    PurchaseFrequencyChart,
    PaidOrdersRateChart,
    RepeatCustomerRateChart
)

from .celery import tasks
from lib.helpers import generate_redis_result_key
from widgets.helpers import get_recommendations_widgets

db = settings.DB


def get_widgets(site_id):
    site = ObjectId(site_id)
    widgets = []
    widgets_configs = WidgetsConf.objects(site=site)
    for widgets_config in widgets_configs:
        json_config = json.loads(widgets_config.config)['widgets']
        for w_id in json_config.keys():
            name = json_config[w_id]['name']
            w_type = json_config[w_id]['type']
            widgets.append({'id': w_id, 'name': name, 'type': w_type})
    return widgets


class Analytics(TemplateView):
    template_name = "analytics/index.html"

    def post(self, request):
        sections = {
            'visits': {
                'label': ugettext(u'По визитам'),
                'link': reverse('analytics.visits')
            },
            'leads': {
                'label': ugettext(u'По клиентам'),
                'link': reverse('analytics.leads')
            },
            'emails': {
                'label': ugettext(u'По рассылкам'),
                'link': reverse('analytics.emails')
            },
            'widgets': {
                'label': ugettext(u'По виджетам'),
                'link': reverse('analytics.widgets')
            },
            'recommendations': {
                'label': ugettext(u'По рекомендациям'),
                'link': reverse('analytics.recommendations')
            },
            'emails_dynamics': {
                'label': ugettext(u'По динамике email\'ов'),
                'link': reverse('analytics.emails_dynamics')
            },
            'autocasts': {
                'label': ugettext(u'По авторассылкам'),
                'link': reverse('analytics.autocasts')
            },
            'sales_funnel': {
                'label': ugettext(u'По воронке продаж'),
                'link': reverse('analytics.sales_funnel')
            },
            'shop_kpi': {
                'label': ugettext(u'KPI'),
                'link': reverse('analytics.shop_kpi')
            },
        }

        return JsonResponse({
            'sections': sections
        })


class VisitsDataMixin(object):

    def process_form(self, post_data):
        p = P()
        options = {'site': self.request.site_id}
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
                p = P.from_def_ranges(period)
        else:
            if not form_data['period_start']:
                form.errors['period_start'] = [ugettext(u'Введите дату.')]
                return {'error': humanize_form_errors(form)}
            if not form_data['period_end']:
                form.errors['period_end'] = [ugettext(u'Введите дату.')]
                return {'error': humanize_form_errors(form)}
            if form_data['period_start'].date() > datetime.now().date():
                form.errors['period_start'] = [ugettext(u"Начало периода не может быть позже текущего дня.")]
                return {'error': humanize_form_errors(form)}
            if form_data['period_end'].date() > datetime.now().date():
                form.errors['period_end'] = [ugettext(u"Конец периода не может быть позже текущего дня.")]
                return {'error': humanize_form_errors(form)}
            if form_data['period_start'] > form_data['period_end']:
                form.errors['period_start'] = [ugettext(u'Начало периода не должно быть больше конца периода.')]
                return {'error': humanize_form_errors(form)}

            p.set_start_and_end(form_data['period_start'], form_data['period_end'])

        options.update({'period_end': p.end,
                        'period_start': p.start,
                        'step': p.step
                        })
        return options

    def get_data(self, options):

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

        return JsonResponse({
            'data': {
                'graph': visits,
                'total_per_period': total
            }
        })

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


class VisitStatView(VisitsDataMixin, TemplateView):
    template_name = "analytics/visits.html"

    @method_decorator(permission_required('analytics'))
    def post(self, request):
        options = self.process_form(request.POST)
        if options.get('error'):
            return JsonResponse(options)
        return self.get_data(options)

    def get_context_data(self):
        system_sources = TraffSource.objects(site=None)
        user_sources = TraffSource.objects(site=self.request.site)
        return {"data": {"system_sources": system_sources,
                         "user_sources": user_sources
                         }
                }


@method_decorator(permission_required('analytics'), name="dispatch")
class WidgetsView(TemplateView):
    template_name = "analytics/widgets.html"

    def get_context_data(self, **kwargs):
        context = super(WidgetsView, self).get_context_data(**kwargs)
        widgets = get_widgets(self.request.site_id)
        config = WidgetsConf.objects(site=self.request.site).first()
        if config:
            tests = json.loads(config.config)['testcases']
            testcases = {}
            [testcases.update(test) for test in tests]
            context.update({'testcases': testcases})
        context.update({'widgets': widgets})

        return context


@method_decorator(role_required('master'), name="dispatch")
@method_decorator(permission_required('analytics'), name="dispatch")
class WidgetStatsView(WidgetsChart, TemplateView):
    template_name = "analytics/widget_stats.html"

    def post(self, request, **kwargs):
        result = self.validate_input(self.request.POST)
        if result['status'] == 'error':
            return JsonResponse(result)
        return JsonResponse(self.get_data(result))


@method_decorator(permission_required('analytics'), name="dispatch")
class LeadsStatsView(LeadsChart, TemplateView):
    template_name = 'analytics/leads.html'

    def post(self, request, **kwargs):
        result = self.validate_input(self.request.POST)
        if result['status'] == 'error':
            return JsonResponse(result)
        return JsonResponse(self.get_data(result, site=self.request.site))

    def get_context_data(self):
        context = super(LeadsStatsView, self).get_context_data()
        system_sources = TraffSource.objects(site=None)
        user_sources = TraffSource.objects(site=self.request.site)
        context.update({
            "data": {
                "system_sources": system_sources,
                "user_sources": user_sources
            }
        })
        return context


class EmailCampaignStatsView(EmailCampaignsChart, TemplateView):
    template_name = "analytics/emails.html"

    def get_context_data(self):
        context = super(EmailCampaignStatsView, self).get_context_data()
        raw_autocasts = Autocast.objects(site=self.request.site).values_list('id', 'name', 'time_added')
        sorted_autocasts = sorted(raw_autocasts, key=lambda x: x[2], reverse=True)
        autocasts = [{'id': str(message[0]), 'name': message[1]} for message in sorted_autocasts]

        raw_messages = Message.objects(
            site=self.request.site, internal__in=(False, None)
        ).values_list('id', 'name', 'sent')
        sorted_messages = sorted(raw_messages, key=lambda x: x[2], reverse=True)
        mass_messages = [{'id': str(message[0]), 'name': message[1]} for message in sorted_messages]

        context.update({
            "autocasts": autocasts,
            "email_campaigns": mass_messages
        })
        return context

    def post(self, request):
        result = self.validate_input(request.POST)
        if result['status'] == 'error':
            return JsonResponse(result)
        data = self.get_data(result)
        return JsonResponse(data)


class EmailsSentStatsView(View):
    def get_data(self, request):
        messages = Message.object(site=request.site)
        match_stage = {
            '$match': {
                'message': {'$in': [DBRef('messages', message.id) for message in messages]}
            }
        }

        group_stage = {
            '$group': {
                '_id': {
                    '$dateToString': {
                        'format': "%Y-%m-%d %H:%mm",
                        'date': "$time_added"
                    }
                },
                'count': {
                    '$sum': 1
                }
            }
        }

        emails = db.emails.aggregate([match_stage, group_stage])

        return emails


@method_decorator(permission_required('analytics'), name="dispatch")
class RecommendationsStatsView(RecommendationsChart, TemplateView):
    template_name = "analytics/recommendations.html"

    def get_context_data(self, **kwargs):
        context = super(RecommendationsStatsView, self).get_context_data(**kwargs)
        context.update({'widgets': get_recommendations_widgets(self.request.site.id)})
        return context

    def post(self, request):
        options = deepcopy(self.request.POST)
        options['site_id'] = self.request.site.id
        result = self.validate_input(options)
        if result['status'] == 'error':
            return JsonResponse(result)
        return JsonResponse(self.get_data(result))


@method_decorator(permission_required('analytics'), name="dispatch")
class EmailDynamicsStatsView(EmailDynamicsChart, TemplateView):
    template_name = "analytics/emails_dynamics.html"

    def post(self, request):
        options = deepcopy(self.request.POST)
        result = self.validate_input(options)
        if result['status'] == 'error':
            return JsonResponse(result)
        result['site_id'] = self.request.site.id
        return JsonResponse(self.get_data(result))


class DashBoardView(VisitsChart, TemplateView):
    template_name = 'analytics/dashboard.html'

    def post(self, request):
        site = request.site
        if request.POST.get('init'):
            dashboard_settings = site.interface_configuration.dashboard
            data = {
                'site_id': str(site.id),
                'period': dashboard_settings['default_period']
            }
        elif request.POST.get('set_default_period'):
            period = request.POST.get('default_period')
            site.interface_configuration.dashboard['default_period'] = period
            site.save()
            data = {'status': 'success'}
        else:
            graph = request.POST.get('graph')
            if graph == 'visits':
                chart = VisitsChart()
            if graph == 'sales_funnel':
                chart = SalesFunnelChart(site_id=site.id)
            if graph == 'sales_bar':
                chart = SalesBarChart(site_id=site.id)
            if graph == 'recommendations':
                chart = RecommendationsChart()
            if graph == 'emails':
                chart = EmailDynamicsChart()
            if graph == 'leads_discovery':
                chart = LeadsDiscoveryChart()

            result = chart.validate_input(request.POST)
            result.update({'site_id': site.id})

            if result.get('status') == 'error':
                return JsonResponse(result)

            result_key = generate_redis_result_key()
            tasks.fetch_chart_data.apply_async(args=[chart, result, result_key], kwargs={'language': get_language()})
            data = {'result_key': result_key}

        return JsonResponse(data)


class AutocastsStatsView(TemplateView):
    template_name = 'analytics/autocasts.html'

    def get_context_data(self, **kwargs):
        context = super(AutocastsStatsView, self).get_context_data(**kwargs)
        context['autocasts'] = Autocast.objects(site=self.request.site).count()
        return context

    def post(self, request):
        booleans_dict = {
            'false': False,
            'true': True
        }

        include_test_emails = booleans_dict[request.POST.get('test_emails')]
        include_archived = booleans_dict[request.POST.get('show_archived')]
        date_format = '%Y-%m-%d'

        start = request.POST.get('period_start')
        end = request.POST.get('period_end')

        if start:
            start = datetime.strptime(start, date_format)
        if end:
            end = datetime.strptime(end, date_format)

        start, end, errors = process_autocasts_query_timeframe(start, end, request.site)
        if errors.values():
            return JsonResponse({'status': 'error', 'errors': errors, 'data': [], 'recordsTotal': 0})

        autocasts = Autocast.objects(site=request.site, message__exists=True)
        if not include_archived:
            autocasts = autocasts(archived__ne=True)

        message_ids, messages_to_autocasts_dict = get_messages_and_msg_to_autocast_dict(autocasts)

        message_ids = [DBRef('messages', mid) for mid in message_ids]

        match = {
            '$match': {
                'message': {'$in': message_ids},
                'lead_id': {'$ne': 'None'},
                'time_added': {'$gte': start, '$lte': end}
            }
        }

        match_test_emails = deepcopy(match)
        match_test_emails['$match']['lead_id'] = 'None'

        group = {
            '$group': {
                '_id': '$message',
                'recipients_count': {'$sum': 1},
                'delivered': {'$sum': {'$cond': [{'$eq': ['$status', 'delivered']}, 1, 0]}},
                'opened': {'$sum': {'$cond': [{'$eq': ['$opened', True]}, 1, 0]}},
                'clicked': {'$sum': {'$cond': [{'$eq': ['$clicked', True]}, 1, 0]}},
                'unsubscribed': {'$sum': {'$cond': [{'$eq': ['$unsubscribed', True]}, 1, 0]}},
            }
        }

        project = {
            '$project': {
                'recipients_count': 1,
                'delivered': 1,
                'clicked': 1,
                'opened': 1,
                'unsubscribed': 1,
                'delivered_percentage': {'$cond': [{'$eq': ['$recipients_count', 0]}, 0, {'$divide': ['$delivered', '$recipients_count']}]},
                'opened_percentage': {'$cond': [{'$eq': ['$delivered', 0]}, 0, {'$divide': ['$opened', '$delivered']}]},
                'clicked_percentage': {'$cond': [{'$eq': ['$opened', 0]}, 0, {'$divide': ['$clicked', '$opened']}]},
                'unsubscribed_percentage': {'$cond': [{'$eq': ['$delivered', 0]}, 0, {'$divide': ['$unsubscribed', '$delivered']}]}
            }
        }

        aggregated_data = db.emails.aggregate([match, group, project])
        aggregated_data = {item['_id'].id: item for item in aggregated_data}

        if include_test_emails:
            test_emails_aggrgegated_data = db.emails.aggregate([match_test_emails, group, project])
            test_emails_aggrgegated_data = {item['_id'].id: item for item in test_emails_aggrgegated_data}

        data = []
        for key in messages_to_autocasts_dict:
            autocast = messages_to_autocasts_dict[key]['autocast']

            formatted_data = {
                'id': str(autocast.id),
                'name': autocast.name,
                'message_name': autocast.message.name,
                'cases': autocast.cases,
                'archived': autocast.archived,
            }

            autocast_aggr_data = aggregated_data.get(key, {})
            stats = {
                'recipients_count': {
                    'real': autocast_aggr_data.get('recipients_count', 0),
                },
                'delivered_emails': {
                    'real': autocast_aggr_data.get('delivered', 0),
                    'percentage': '{:.3g}%'.format(autocast_aggr_data.get('delivered_percentage', 0) * 100)
                },
                'opened_emails': {
                    'real': autocast_aggr_data.get('opened', 0),
                    'percentage': '{:.3g}%'.format(autocast_aggr_data.get('opened_percentage', 0) * 100)
                },
                'clicked_emails': {
                    'real': autocast_aggr_data.get('clicked', 0),
                    'percentage': '{:.3g}%'.format(autocast_aggr_data.get('clicked_percentage', 0) * 100)
                },
                'unsubscribed': {
                    'real': autocast_aggr_data.get('unsubscribed', 0),
                    'percentage': '{:.3g}%'.format(autocast_aggr_data.get('unsubscribed_percentage', 0) * 100)
                }
            }

            if include_test_emails:
                autocast_test_data_aggregated = test_emails_aggrgegated_data.get(key, {})
                stats['recipients_count']['test'] = autocast_test_data_aggregated.get('recipients_count', 0)
                stats['delivered_emails']['test'] = autocast_test_data_aggregated.get('delivered', 0)
                stats['opened_emails']['test'] = autocast_test_data_aggregated.get('opened', 0)
                stats['clicked_emails']['test'] = autocast_test_data_aggregated.get('clicked', 0)

            formatted_data.update(stats)

            if autocast.cases:
                cases = []
                for case in autocast.cases:
                    msg_id = case.message.id
                    case_data = {
                        'name': case.name,
                        'message_name': case.message.name
                    }
                    case_aggr_data = aggregated_data.get(msg_id, {})
                    case_data.update({
                        'name': case.name,
                        'message_name': case.message.name,
                        'recipients_count': {
                            'real': case_aggr_data.get('recipients_count', 0),
                        },
                        'delivered_emails': {
                            'real': case_aggr_data.get('delivered', 0),
                            'percentage': '{:.3g}%'.format(case_aggr_data.get('delivered_percentage', 0) * 100)
                        },
                        'opened_emails': {
                            'real': case_aggr_data.get('opened', 0),
                            'percentage': '{:.3g}%'.format(case_aggr_data.get('opened_percentage', 0) * 100)
                        },
                        'clicked_emails': {
                            'real': case_aggr_data.get('clicked', 0),
                            'percentage': '{:.3g}%'.format(case_aggr_data.get('clicked_percentage', 0) * 100)
                        },
                        'unsubscribed': {'real': 0}
                    })
                    if include_test_emails:
                        case_test_data_aggregated = test_emails_aggrgegated_data.get(msg_id, {})
                        case_data['recipients_count']['test'] = case_test_data_aggregated.get('recipients_count', 0)
                        case_data['delivered_emails']['test'] = case_test_data_aggregated.get('delivered', 0)
                        case_data['opened_emails']['test'] = case_test_data_aggregated.get('opened_emails', 0)
                        case_data['clicked_emails']['test'] = case_test_data_aggregated.get('clicked', 0)

                    cases.append(case_data)
                    for case in cases[1:]:
                        formatted_data['recipients_count']['real'] += case['recipients_count']['real']
                        formatted_data['delivered_emails']['real'] += case['delivered_emails']['real']
                        formatted_data['opened_emails']['real'] += case['opened_emails']['real']
                        formatted_data['clicked_emails']['real'] += case['clicked_emails']['real']
                    if include_test_emails and key in aggregated_data:
                        for case in cases[1:]:
                            formatted_data['recipients_count']['test'] += case['recipients_count']['test']
                            formatted_data['delivered_emails']['test'] += case['delivered_emails']['test']
                            formatted_data['opened_emails']['test'] += case['opened_emails']['test']
                            formatted_data['clicked_emails']['test'] += case['clicked_emails']['test']
                formatted_data['cases'] = cases
            data.append(formatted_data)

        sort_fields = [
            lambda x: x['name'],
            lambda x: x['recipients_count']['real'],
            lambda x: x['delivered_emails']['real'],
            lambda x: x['opened_emails']['real'],
            lambda x: x['clicked_emails']['real'],
            lambda x: x['unsubscribed']['real'],
        ]

        sort_directions = {
            'asc': False,
            'desc': True
        }

        per_page = int(request.POST['length'])
        page = int(int(request.POST['start']) / per_page) + 1

        sort_direction = sort_directions[request.POST['order[0][dir]']]
        field = sort_fields[int(request.POST['order[0][column]'])]

        data.sort(key=field, reverse=sort_direction)
        autocasts_list = Pagination(iterable=data, page=page, per_page=per_page)

        return JsonResponse({
            'data': autocasts_list.items,
            'status': 'ok',
            'recordsTotal': autocasts_list.total,
            'recordsFiltered': autocasts_list.total,
            'draw': request.POST['draw'],
        })


class SalesFunnelStatsView(SalesFunnelChart, TemplateView):
    template_name = 'analytics/sales_funnel.html'

    def post(self, request):
        result = self.validate_input(request.POST)
        if result['status'] == 'error':
            return JsonResponse(result)
        return JsonResponse(self.get_data(result))


class ShopKPIView(TemplateView):
    template_name = 'analytics/shops_kpi.html'

    def post(self, request):
        validation_result = validate_time_period(request.POST.get('period_start'),
                                                 request.POST.get('period_end'), date_format='%d.%m.%Y')
        if validation_result['status'] == 'error':
            return JsonResponse(validation_result)

        period_start = validation_result['start']
        period_end = validation_result['end']

        current_week, current_day = datetime.now().isocalendar()[1:]

        if (period_end - period_start).days < 6:
            if not(period_start.isocalendar()[1:] == (current_week, 1) and
                   period_end.isocalendar()[1] == current_week and period_end.isocalendar()[2] >= current_day):
                return JsonResponse({
                    'status': 'error',
                    'errors': {ugettext(u'Период'): [ugettext(u'Минимальный период для выбора составляет 7 дней')]}
                })

        result_key = generate_redis_result_key()
        tasks.get_shop_metrics.apply_async(
            args=[self.request.site_id, period_start, period_end, result_key]
        )

        return JsonResponse({'result_key': result_key})


class ShopKPIGraphDataView(AverageRevenuePerVisitorChart, View):

    def post(self, request):
        validation_result = validate_time_period(request.POST.get('period_start'),
                                                 request.POST.get('period_end'), date_format='%d.%m.%Y')
        if validation_result['status'] == 'error':
            return JsonResponse(validation_result)

        period_start = validation_result['start']
        period_end = validation_result['end']

        current_week, current_day = datetime.now().isocalendar()[1:]

        if (period_end - period_start).days < 6:
            if not(period_start.isocalendar()[1:] == (current_week, 1) and
                   period_end.isocalendar()[1] == current_week and period_end.isocalendar()[2] >= current_day):
                return JsonResponse({
                    'status': 'error',
                    'errors': {ugettext(u'Период'): [ugettext(u'Минимальный период для выбора составляет 7 дней')]}
                })

        period_start = period_start - timedelta(days=period_start.weekday())
        period_end = period_end + timedelta(days=6 - period_end.weekday())
        graph_type = request.POST.get('graph_type')

        charts = {
            'arpv': AverageRevenuePerVisitorChart,
            'arpu': AverageRevenuePerUserChart,
            'arppu': AverageRevenuePerPayingUserChart,
            'car': CartAbandonmentRateChart,
            'average_check': AverageCheckChart,
            'purchase_frequency': PurchaseFrequencyChart,
            'paid_orders_rate': PaidOrdersRateChart,
            'rcr': RepeatCustomerRateChart
        }
        chart = charts[graph_type]()
        result_key = generate_redis_result_key()
        tasks.fetch_chart_data.apply_async(
            args=[
                chart,
                {
                    'period_start': period_start,
                    'period_end': period_end,
                    'site_id': self.request.site.id
                },
                result_key
            ]
        )

        return JsonResponse({'result_key': result_key})

