# coding: utf-8
import json

from django.shortcuts import redirect, render
from django.core.urlresolvers import reverse
from django.contrib.auth import login, logout
from django.views.generic import View
from django.views.decorators.csrf import csrf_exempt
from django.http import JsonResponse, HttpResponse
from django.conf import settings
from django.utils.translation import ugettext_lazy as _
from accounts.registration import ShopifyUserRegistration
from leads.models import Lead, LeadOrder, Multilead
from automailer.coupon_model import Coupon
from accounts.utils import check_spf as check_site_spf
from accounts.models import NewUser as User, Site, Tariff, YMLFile, YMLOffer, WidgetsConf
from profile.views import LeadForm, Leadlist, EmailTemplate, Autocast
from django.utils.decorators import decorator_from_middleware, method_decorator
from middleware import LoginProtection
from profile.views import TRACKER_CODE
from profile.celery.yml import upload_yml_file
from lib.core.acl import role_required, permission_required

from lxml import etree

import string
import random
import shopify
import datetime
import logging

shopify_logger = logging.getLogger('shopify.request')


@method_decorator(decorator_from_middleware(LoginProtection), name='dispatch')
class LoginView(View):

    http_method_names = ['get']

    def get(self, request):
        try:
            shop = request.GET.get('shop')
            redirect_uri = request.build_absolute_uri(reverse('shopify_app:finalize'))
            if 'https' not in redirect_uri:
                redirect_uri = redirect_uri.replace('http', 'https')
            permission_url = shopify.Session(shop.strip()).create_permission_url(settings.SHOPIFY_API_SCOPE, redirect_uri)
            return redirect(permission_url)
        except:
            return JsonResponse({'status': 'error',
                                 'error': _(u'Некорректное имя магазина')})


@method_decorator(decorator_from_middleware(LoginProtection), name='dispatch')
class FinalizeView(View):

    http_method_names = ['get']

    def get(self, request):

        shop = request.GET.get('shop')
        tariff = Tariff.objects(name='Shopify')
        if not tariff:
            Tariff(name='Shopify', emails_limit=5000, leads_limit=999999999, sites_limit=1,
                   forms_limit=30, alerts_limit=5000, scopes=[u'leads', u'email', u'automailer',
                                                              u'widgets', u'reports', u'analytics',
                                                              u'lists', u'smm', u'alerts']).save()
        try:
            shopify_session = shopify.Session(shop)
            access_token = shopify_session.request_token(request.GET)
        except:
            return JsonResponse({'status': 'error',
                                 'error': _(u'Некорретные данные запроса')})

        with shopify.Session.temp(shop, access_token):

            current_shop = shopify.Shop.current()
            site_name = 'https://' + current_shop.domain

            site = Site.objects(domain=site_name).first()

            if site:
                # Login request
                charge = shopify.RecurringApplicationCharge.current()
                try:
                    status = charge.attributes['status']
                    if status == 'declined' or status == 'frozen':
                        site.update(is_active=False)
                        return render(request, 'shopify_info.html', {'store_url': site.domain + '/admin/apps'})
                    # charge is fine case
                    if status == 'accepted':
                        charge.activate()
                    if not site.is_active:
                        site.update(is_active=True)
                    user = User.objects(sites__in=[str(site.id)]).first()
                    if not user.is_active:
                        user.update(is_active=True)
                except AttributeError:
                    # current() возвращает None
                    # Согласно документации, данная ситуация возникает в случае если спустя 48 часов после выставления счета магазин его не оплачивает
                    # В случае с тестовым магазином данный случай протестировать нереально.
                    # Также это кейс возможен в случае отказа от платежа при установке и последющей попытке зайти в сервис
                    site.update(shopify_token=access_token)  # Токен обновляется на случай если, магазин удалил наше приложение и установил заново

                    charge = shopify.RecurringApplicationCharge()
                    charge.price = 29
                    charge.name = 'LeadHit installation charge'
                    if current_shop.attributes['plan_name'] == 'affiliate':
                        charge.test = True
                    # Также такой случай возможен в случае удаления и последующей установки магазином нашего приложения
                    # Ниже обрабатывается именно этот случай
                    site_added = site.time_added
                    trial_expire_date = site_added + datetime.timedelta(days=30)
                    now = datetime.datetime.now()
                    if now < trial_expire_date:
                        trial_days_left = (trial_expire_date - now).days
                        charge.trial_days = trial_days_left

                    return_url = request.build_absolute_uri(reverse('shopify_app:process_charge'))
                    if 'https' not in return_url:
                        return_url = return_url.replace('http', 'https')
                    charge.return_url = return_url + '?shop=' + site_name
                    charge.save()

                    confirmation_url = charge.attributes['confirmation_url']
                    return redirect(confirmation_url)

                user = User.objects.get(sites=str(site.id), role='master')
                if not user.is_active:
                    user.update(is_active=True)
                site.update(shopify_token=access_token)
                if not request.user.is_anonymous:
                    logout(request)
                return_url = request.build_absolute_uri(reverse('shopify_app:process_charge'))

                self.check_or_update_tracker(request, str(site.id))
                login(request, user)
                return redirect('/?site_id={}'.format(site.id))
            else:
                # Registration request
                username = current_shop.email
                password = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(5))
                phone = current_shop.phone
                tariff_name = 'Shopify'
                host = ''.join([request.scheme, '://', request.get_host()])
                registration = ShopifyUserRegistration(username, password, phone, tariff_name, site_name, host)
                user, site = registration.register()
                site.update(shopify_token=access_token)
                yml_file = YMLFile.objects.get(site=site)

                # create the default coupon for a site
                Coupon(
                    code='LeadHit10OFF',
                    text='Get a discount!',
                    categories={},
                    instruction='Paste this code on the checkout page to get a discount',
                    fit_to_random=True,
                    yml_file=yml_file,
                    site=site,
                    ignore_availability=False,
                    universal=True).save()

                charge = shopify.RecurringApplicationCharge()
                charge.price = 29
                charge.name = 'LeadHit app installation'
                charge.trial_days = 30
                if current_shop.attributes['plan_name'] == 'affiliate':
                    charge.test = True
                return_url = request.build_absolute_uri(reverse('shopify_app:process_charge'))
                if 'https' not in return_url:
                    return_url = return_url.replace('http', 'https')
                charge.return_url = return_url + '?shop=' + site_name
                charge.save()

                # getting charge confirmation_url after saving
                confirmation_url = charge.attributes['confirmation_url']
                return redirect(confirmation_url)

    def check_or_update_tracker(self, request, site_id):
        script_url = request.build_absolute_uri(reverse('shopify_app:site_tracker')) + '?site_id={}'.format(site_id)
        script_url = script_url.replace('http://', 'https://')
        script_tag = shopify.ScriptTag.find(src=script_url)
        if not script_tag:
            script_tag = shopify.ScriptTag()
            script_tag.src = script_url
            script_tag.event = 'onload'
            script_tag.save()


# @method_decorator(decorator_from_middleware(LoginProtection), name='dispatch')
class ProcessChargeView(View):
    http_method_names = ['get', 'options']

    def get(self, request):
        shop = request.GET.get('shop')
        charge_id = request.GET.get('charge_id')
        site = Site.objects(domain=shop).first()

        if not site:
            response = render(request, 'shopify_info.html', {'store_url': shop + '/admin/apps', 'message': 'Unknown error occurred. Send us feedback at support@leadhit.io'})
            response['Access-Control-Allow-Origin'] = '*'
            return response

        yml_file = YMLFile.objects.get(site=site)
        with shopify.Session.temp(shop, site.shopify_token):
            charge = shopify.RecurringApplicationCharge.find(charge_id)
            webhook = shopify.Webhook()
            webhook.topic = 'app/uninstalled'
            webhook.address = 'https://service.leadhit.ru/shopify/app_delete/'
            webhook.save()
            if charge.attributes['status'] == 'accepted':
                charge.activate()
                redirect_uri = request.build_absolute_uri(reverse('shopify_app:login'))
                redirect_uri = redirect_uri.replace('http', 'https') + '?shop=' + shop + '&charge_id=' + charge_id
                response = redirect(redirect_uri)
                upload_yml_file.apply_async(args=[yml_file, 'force'])
                autocasts = Autocast.objects(site=site)
                for a in autocasts:
                    a.sender = u'shopify@leadhit.io'
                    a.save()
            else:
                response = render(request, 'shopify_info.html', {
                    'store_admin': site.domain + '/admin/apps'
                })
                site.is_active = False
                site.save()

        response['Access-Control-Allow-Origin'] = '*'
        return response

    def options(self, request):
        response = HttpResponse('')
        response['Access-Control-Allow-Origin'] = '*'
        response['Access-Control-Allow-Methods'] = 'GET'
        response['Access-Control-Allow-Headers'] = 'x-shopify-web'
        return response


class SiteTrackerView(View):

    http_method_names = ['get']

    def get(self, request):
        site_id = request.GET.get('site_id', '')
        code = TRACKER_CODE.format(site_id=site_id)
        return HttpResponse(code, content_type='application/javascript')


@method_decorator(decorator_from_middleware(LoginProtection), name='dispatch')
class FakeYMLView(View):

    def get(self, request):

        site = Site.objects.get(domain=request.GET.get('site'))
        shop = site.domain.replace('https://', '')
        access_token = site.shopify_token
        with shopify.Session.temp(shop, access_token):
            shopify_shop_obj = shopify.Shop.current()
            shop_collects = shopify.Collect.find()
            shop_products = shopify.Product.find()
            shop_categories = shopify.SmartCollection.find()
            shop_categories += shopify.CustomCollection.find()
            yml_file = generate_yml_file(shopify_shop_obj, shop_categories, shop_products, shop_collects)
        return HttpResponse(yml_file, content_type='text/xml')


def generate_yml_file(shopify_shop, shop_categories, products, collects):
    now = str(datetime.datetime.now()).rsplit(':', 1)[0]
    # setup root element
    root = etree.Element('yml_catalog', date=now)

    # setup shop subelement and it`s subelements 'name', 'company', 'url' and 'currency'
    shop = etree.SubElement(root, 'shop')
    shop_name = etree.SubElement(shop, 'name')
    shop_name.text = shopify_shop.attributes.get('name')
    shop_company = etree.SubElement(shop, 'company')
    shop_company.text = shopify_shop.attributes.get('name')
    shop_url = etree.SubElement(shop, 'url')
    shop_url.text = 'https://' + shopify_shop.attributes.get('domain')
    shop_currencies = etree.SubElement(shop, 'currencies')
    shop_single_currency = etree.SubElement(shop_currencies, 'currency', id=shopify_shop.attributes.get('currency', 'USD'))
    offers = etree.SubElement(shop, 'offers')

    categories = {category.id: category for category in shop_categories}
    categories_tag = etree.SubElement(shop, 'categories')
    for category in categories:
        cat_obj = categories[category]
        cur_category = etree.SubElement(categories_tag, 'category', id=str(cat_obj.id))
        cur_category.text = cat_obj.attributes['title']
    default_category = etree.SubElement(categories_tag, 'category', id='Main')

    # data for yml offers to add
    products = {product.id: product for product in products}
    processed_products = set()
    shop_domain = shopify_shop.attributes['domain']
    currencyId = shopify_shop.attributes.get('currency', 'USD')

    for category in shop_categories:
        category_products = category.products()
        for product in category_products:
            add_yml_offer(parent_element=offers, product=product, category=category, shop_domain=shop_domain, currencyId=currencyId)
            processed_products.add(product.id)

    # removing processed products belonging to any category
    # products having no category stay in products dict and get assigned the default category
    for processed_product in processed_products:
        if processed_product in products:
            del products[processed_product]

    for prd_id in products:
        product = products[prd_id]
        add_yml_offer(parent_element=offers, product=product, category=None, shop_domain=shop_domain, currencyId=currencyId)

    return etree.tostring(root, pretty_print=True)


def add_yml_offer(parent_element=None, product=None, category=None, shop_domain=None, currencyId=None):
    if not category:
        # offers with no category get assigned "Main" category
        add_offer(parent_element=parent_element, product=product, category=category, shop_domain=shop_domain, currencyId=currencyId)
    else:
        add_offer(based_on_category=False, parent_element=parent_element, product=product, category=category, shop_domain=shop_domain, currencyId=currencyId)
        add_offer(parent_element=parent_element, product=product, category=category, shop_domain=shop_domain, currencyId=currencyId)


def add_offer(based_on_category=True, parent_element=None, product=None, category=None, shop_domain=None, currencyId=None):
    for variant in product.variants:
        availability = 'true' if variant.inventory_quantity else 'false'
        cur_product = etree.SubElement(parent_element, 'offer', parent_id=str(product.id), id=str(variant.id), available=availability)

        name = product.attributes['title'] if variant.title == 'Default Title' else product.attributes['title'] + ', ' + variant.title
        product_name = etree.SubElement(cur_product, 'name')
        product_name.text = name

        product_url = etree.SubElement(cur_product, 'url')

        if based_on_category and category:
            product_url.text = 'https://' + shop_domain + '/collections/' + category.handle + '/products/' + product.attributes['handle']
        else:
            product_url.text = 'https://' + shop_domain + '/products/' + product.attributes['handle']

        product_price = etree.SubElement(cur_product, 'price')
        product_price.text = variant.attributes['price']

        product_currency = etree.SubElement(cur_product, 'currencyId')
        product_currency.text = currencyId

        if product.attributes.get('image'):
            product_picture = etree.SubElement(cur_product, 'picture')
            image_url = product.attributes['image'].attributes['src']
            if '?v=' in image_url:
                image_url = image_url[:image_url.find('?v=')]
            product_picture.text = image_url

        product_category = etree.SubElement(cur_product, 'categoryId')
        if category:
            product_category.text = str(category.id)
        else:
            product_category.text = 'Main'


@role_required(('master', 'manager'))
def get_or_set_site_settings(request):
    site = request.site
    if request.method == 'GET':
        return JsonResponse({
            'logo_url': site.logo_url,
            'cart_url': site.domain
        })

    if request.method == 'POST':
        attr = request.POST.get('attr')
        value = request.POST.get('value')
        setattr(site, attr, value)
        site.save()
        return JsonResponse({'status': 'success', 'msg': 'site settings have been updated'})


def check_spf(request):
    site = request.site
    check_site_spf(site)
    if site.spf_correct and 'shopify' in get_store_second_level_domain(site.domain):
        site.spf_correct = False
        site.save()
    return JsonResponse({'spf_correct': site.spf_correct})


@csrf_exempt
def delete_shop(request):
    shopify_logger.info(request.path + '\n' + request.body + '\n' + '*' * 50)
    return HttpResponse(status=200)


@csrf_exempt
def customer_redact(request):
    shopify_logger.info(request.path + '\n' + request.body + '\n' + '*' * 50)
    return HttpResponse(status=200)


@csrf_exempt
def get_customer_data(request):
    """ request payload
    {
        "shop_id":22747251,
        "shop_domain":"lead-test.myshopify.com",
        "customer":{"id":688988520501,"email":"test@mail.ru","phone":null},"orders_requested":[589347782709,589486522421,589545340981]
    }
    """
    data = json.loads(request.body)
    try:
        site = Site.objects.get(domain='https://' + data['shop_domain'])
        lead = Lead.objects.get(site=site, email=data['customer']['email'])
        # orders = list(LeadOrder.objects(lead=lead))
        orders = [
            {'cart_sum': order.cart_sum,
             'time_submitted': order.time_added,
             'order_id': order.order_id} for order in LeadOrder.objects(lead=lead)]
    except:
        return JsonResponse({})
    return JsonResponse({'email': lead.email, 'orders': orders})


@csrf_exempt
def app_delete(request):
    '''
    Gets triggered by our application removal.
    Deactivates user if none of his sites have our app installed
    '''
    body = json.loads(request.body)
    site_domain = 'https://' + body['domain']
    site = Site.objects(domain=site_domain).first()
    if site is None:
        return HttpResponse(status=200)
    site.is_active = False
    site.save()

    user = User.objects(sites__in=[str(site.id)]).first()
    no_active_sites = not any(site.is_active for site in Site.objects(id__in=user.sites))
    if no_active_sites:
        user.is_active = False
        user.save()
    return HttpResponse(status=200)


def get_store_second_level_domain(address):
    return address.strip('/').split('.')[-2]
