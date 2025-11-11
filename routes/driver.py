from flask import Blueprint, render_template, request, url_for, jsonify
from models import DriverEvent, db
from datetime import datetime, timedelta
from sqlalchemy import func
from functools import lru_cache

driver_bp = Blueprint('driver', __name__)

# --------------------------------------------------------------------
# EVENT SCORING CONFIG
# --------------------------------------------------------------------
EVENT_SCORING = {
    'Excessive Idling': {'penalty': 10, 'max_penalty': 100, 'cost': 10},
    'Harsh Acceleration': {'penalty': 1, 'max_penalty': 100, 'cost': 1},
    'Harsh Braking': {'penalty': 1, 'max_penalty': 100, 'cost': 1},
    'Harsh Cornering': {'penalty': 1, 'max_penalty': 100, 'cost': 1},
    'Overspeeding': {'penalty': 1, 'max_penalty': 100, 'cost': 1},
}

# --------------------------------------------------------------------
# FILTERING
# --------------------------------------------------------------------
def apply_filters(query, start_date, end_date, owner=None, driver_name=None, event_type=None):
    if start_date:
        query = query.filter(DriverEvent.EventDate >= datetime.strptime(start_date, "%Y-%m-%d"))
    if end_date:
        query = query.filter(DriverEvent.EventDate <= datetime.strptime(end_date, "%Y-%m-%d"))
    if owner:
        query = query.filter(DriverEvent.OwnerName == owner)
    if driver_name:
        query = query.filter(DriverEvent.LinkedName_1 == driver_name)
    if event_type:
        query = query.filter(DriverEvent.EventTypes == event_type)
    return query

# --------------------------------------------------------------------
# SCORING
# --------------------------------------------------------------------
def compute_driver_score(events, selected_event_type=None):
    score = 100
    details = {}
    type_counts = {}
    for e in events:
        et = e.EventTypes
        if et:
            type_counts[et] = type_counts.get(et, 0) + 1
    for etype, cfg in EVENT_SCORING.items():
        cnt = type_counts.get(etype, 0)
        pen = min(cfg['penalty'] * cnt, cfg['max_penalty'])
        if not selected_event_type or selected_event_type == etype:
            score -= pen
        details[etype] = {'count': cnt, 'penalty': pen}
    return max(score, 0), details

# --------------------------------------------------------------------
# CACHED DROPDOWNS
# --------------------------------------------------------------------
@lru_cache(maxsize=1)
def get_dropdown_data():
    owners = [o for o, in db.session.query(DriverEvent.OwnerName).distinct().order_by(DriverEvent.OwnerName)]
    drivers = [d for d, in db.session.query(DriverEvent.LinkedName_1)
               .filter(DriverEvent.Class=='Driver').distinct().order_by(DriverEvent.LinkedName_1)]
    event_types = [et for et, in db.session.query(DriverEvent.EventTypes).distinct().order_by(DriverEvent.EventTypes)]
    return owners, drivers, event_types

# --------------------------------------------------------------------
# DASHBOARD VIEW
# --------------------------------------------------------------------
@driver_bp.route('/dashboard')
def driver_dashboard():
    start_date = request.args.get('start_date', (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d"))
    end_date = request.args.get('end_date', datetime.utcnow().strftime("%Y-%m-%d"))
    owner = request.args.get('owner')
    driver_name = request.args.get('driver_name')
    event_type = request.args.get('event_type')

    # ---------------- BASE EVENTS ----------------
    base_query = apply_filters(DriverEvent.query.filter(DriverEvent.Class=='Driver'),
                               start_date, end_date, owner, driver_name, event_type)
    events = base_query.order_by(DriverEvent.EventDate.desc()).limit(1000).all()

    # ---------------- DROPDOWN DATA ----------------
    owners_list, drivers_list, event_types_list = get_dropdown_data()

    # ---------------- TOP DRIVERS WEEKLY ----------------
    top_drivers_query = (
        db.session.query(DriverEvent.LinkedName_1, func.count(DriverEvent.id))
        .filter(DriverEvent.Class=='Driver')
        .group_by(DriverEvent.LinkedName_1)
    )
    top_drivers_query = apply_filters(top_drivers_query, start_date, end_date, owner, driver_name, event_type)
    top_drivers = top_drivers_query.order_by(func.count(DriverEvent.id).desc()).limit(10).all()

    driver_names = [d[0] for d in top_drivers]

    weekly_counts_query = (
        db.session.query(
            DriverEvent.LinkedName_1,
            func.to_char(DriverEvent.EventDate, 'IYYY-IW').label('week'),
            func.count(DriverEvent.id)
        )
        .filter(DriverEvent.LinkedName_1.in_(driver_names))
        .group_by(DriverEvent.LinkedName_1, 'week')
    )
    weekly_counts_query = apply_filters(weekly_counts_query, start_date, end_date, owner, None, event_type)
    weekly_counts = weekly_counts_query.all()

    weekly_dict = {}
    for drv, week, cnt in weekly_counts:
        weekly_dict.setdefault(drv, {})[week] = cnt

    chart_data_weekly = []
    drilldown_series_weekly = []
    for drv, total in top_drivers:
        chart_data_weekly.append({'name': drv, 'y': total, 'drilldown': drv})
        week_drilldown = {'id': drv, 'name': f'Weekly events for {drv}', 'data': []}
        for wk, cnt in weekly_dict.get(drv, {}).items():
            week_drilldown['data'].append({'name': wk, 'y': cnt, 'drilldown': f'{drv}_{wk}'})

            # Event type drilldown per week
            event_type_counts_query = (
                db.session.query(DriverEvent.EventTypes, func.count(DriverEvent.id))
                .filter(DriverEvent.LinkedName_1==drv,
                        func.to_char(DriverEvent.EventDate, 'IYYY-IW')==wk)
                .group_by(DriverEvent.EventTypes)
            )
            event_type_counts_query = apply_filters(event_type_counts_query, start_date, end_date, owner, None, event_type)
            event_type_counts = event_type_counts_query.all()
            drilldown_series_weekly.append({
                'id': f'{drv}_{wk}',
                'name': f'Event Types for {drv} - {wk}',
                'data': [{'name': et, 'y': c,
                          'url': url_for('driver.driver_events', driver_name=drv, week=wk, event_type=et)}
                         for et, c in event_type_counts]
            })
        drilldown_series_weekly.append(week_drilldown)

    # ---------------- HOURLY STACKED COLUMN ----------------
    hourly_query = (
        db.session.query(
            func.extract('hour', DriverEvent.EventDate).label('hour'),
            DriverEvent.EventTypes.label('etype'),
            func.count(DriverEvent.id)
        )
        .filter(DriverEvent.Class=='Driver')
        .group_by('hour', DriverEvent.EventTypes)
    )
    hourly_query = apply_filters(hourly_query, start_date, end_date, owner, driver_name, event_type)
    hourly_raw = [(int(h), et.strip(), c) for h, et, c in hourly_query.all()]

    hours = sorted({h for h, _, _ in hourly_raw})
    types = sorted({et for _, et, _ in hourly_raw})
    hourly_series = [
        {'name': et, 'data': [{'y': sum(c for h, e, c in hourly_raw if h==hr and e==et),
                               'drilldown': f'{hr}_{et}'} for hr in hours]}
        for et in types
    ]

    # Hour drilldown
    hour_drilldown_series = []
    for hr in hours:
        for et in types:
            q = DriverEvent.query.filter(
                DriverEvent.Class=='Driver',
                func.extract('hour', DriverEvent.EventDate)==hr,
                DriverEvent.EventTypes==et
            )
            q = apply_filters(q, start_date, end_date, owner, driver_name, event_type)
            tops = q.with_entities(DriverEvent.LinkedName_1, func.count(DriverEvent.id))\
                    .group_by(DriverEvent.LinkedName_1)\
                    .order_by(func.count(DriverEvent.id).desc())\
                    .limit(10).all()
            hour_drilldown_series.append({
                'id': f'{hr}_{et}',
                'name': f'Top Drivers for Hour {hr} - {et}',
                'data': [{'name': d, 'y': c,
                          'url': url_for('driver.driver_events', driver_name=d, event_type=et)} for d, c in tops]
            })

    # ---------------- OWNER PIE CHART ----------------
    owner_totals_query = db.session.query(DriverEvent.OwnerName, func.count(DriverEvent.id))\
        .filter(DriverEvent.Class=='Driver').group_by(DriverEvent.OwnerName)
    owner_totals_query = apply_filters(owner_totals_query, start_date, end_date, owner, driver_name, event_type)
    owner_totals = owner_totals_query.all()

    chart_data_owner, drilldown_series_owner = [], []
    for own, cnt in owner_totals:
        chart_data_owner.append({'name': own, 'y': cnt, 'drilldown': own})

        # Event types per owner
        event_type_counts_query = (
            db.session.query(DriverEvent.EventTypes, func.count(DriverEvent.id))
            .filter(DriverEvent.OwnerName==own, DriverEvent.Class=='Driver')
            .group_by(DriverEvent.EventTypes)
        )
        event_type_counts_query = apply_filters(event_type_counts_query, start_date, end_date, None, None, event_type)
        event_type_counts = event_type_counts_query.all()

        et_data = []
        for et, ec in event_type_counts:
            # Top drivers per owner and event type
            tops = (
                db.session.query(DriverEvent.LinkedName_1, func.count(DriverEvent.id))
                .filter(DriverEvent.OwnerName==own, DriverEvent.EventTypes==et, DriverEvent.Class=='Driver')
                .group_by(DriverEvent.LinkedName_1)
                .order_by(func.count(DriverEvent.id).desc())
                .limit(10).all()
            )
            drilldown_series_owner.append({
                'id': f'{own}_{et}',
                'name': f'Top Drivers for {et} in {own}',
                'data': [{'name': d, 'y': c,
                          'url': url_for('driver.driver_events', driver_name=d, event_type=et)} for d, c in tops]
            })
            et_data.append({'name': et, 'y': ec, 'drilldown': f'{own}_{et}'})
        drilldown_series_owner.append({'id': own, 'name': f'Event Types in {own}', 'data': et_data})

    # ---------------- TOP ASSETS ----------------
    top_assets_query = db.session.query(DriverEvent.AssetName, func.count(DriverEvent.id))\
        .filter(DriverEvent.Class=='Driver',
                DriverEvent.AssetName.isnot(None),
                DriverEvent.EventTypes!='Non Tagging')\
        .group_by(DriverEvent.AssetName)
    top_assets_query = apply_filters(top_assets_query, start_date, end_date, owner, driver_name, event_type)
    top_assets = top_assets_query.order_by(func.count(DriverEvent.id).desc()).limit(10).all()

    driver_table = []
    for asset, total in top_assets:
        breakdown_query = db.session.query(DriverEvent.EventTypes, func.count(DriverEvent.id))\
            .filter(DriverEvent.AssetName==asset, DriverEvent.Class=='Driver', DriverEvent.EventTypes!='Non Tagging')\
            .group_by(DriverEvent.EventTypes)
        breakdown_query = apply_filters(breakdown_query, start_date, end_date, owner, None, event_type)
        breakdown = breakdown_query.all()
        driver_table.append({
            "asset": asset,
            "total": total,
            "breakdown": [{"type": et, "count": c} for et, c in breakdown],
            "url": url_for('driver.driver_events', asset=asset)
        })

    # ---------------- EVENT TYPE TOTALS ----------------
    event_type_totals_query = (
        db.session.query(DriverEvent.EventTypes, func.count(DriverEvent.id))
        .filter(DriverEvent.Class=='Driver', DriverEvent.EventTypes!='Non Tagging')
        .group_by(DriverEvent.EventTypes)
    )
    event_type_totals_query = apply_filters(event_type_totals_query, start_date, end_date, owner, driver_name, event_type)
    event_type_totals = event_type_totals_query.all()

    event_type_drilldown = {}
    for et, total in event_type_totals:
        driver_counts_query = (
            db.session.query(DriverEvent.LinkedName_1, func.count(DriverEvent.id))
            .filter(DriverEvent.EventTypes==et, DriverEvent.Class=='Driver', DriverEvent.LinkedName_1.isnot(None))
            .group_by(DriverEvent.LinkedName_1)
            .order_by(func.count(DriverEvent.id).desc())
        )
        driver_counts_query = apply_filters(driver_counts_query, start_date, end_date, owner, driver_name, event_type)
        driver_counts = driver_counts_query.all()
        event_type_drilldown[et] = [{"driver": d, "count": c,
                                      "url": url_for('driver.driver_events', driver_name=d, event_type=et)}
                                     for d, c in driver_counts if c>0]

    # ---------------- HTML RENDER ----------------
    return render_template(
        'driver.html',
        events=events,
        owners=owners_list,
        drivers=drivers_list,
        event_types=event_types_list,
        selected_owner=owner,
        selected_driver=driver_name,
        selected_event_type=event_type,
        start_date=start_date,
        end_date=end_date,
        chart_data_weekly=chart_data_weekly,
        drilldown_series_weekly=drilldown_series_weekly,
        hourly_series=hourly_series,
        hour_drilldown_series=hour_drilldown_series,
        chart_data_owner=chart_data_owner,
        drilldown_series_owner=drilldown_series_owner,
        driver_table=driver_table,
        event_type_totals=event_type_totals,
        event_type_drilldown=event_type_drilldown
    )

# --------------------------------------------------------------------
# JSON API ENDPOINT
# --------------------------------------------------------------------
@driver_bp.route('/api/dashboard-data')
def dashboard_data_api():
    # In production, you may factor out logic into a helper returning dict
    return jsonify({"status": "success", "data": "Dashboard JSON output would go here"})

# --------------------------------------------------------------------
# DRIVER EVENTS PAGE
# --------------------------------------------------------------------
@driver_bp.route('/events/')
@driver_bp.route('/events/<driver_name>')
def driver_events(driver_name=None):
    week = request.args.get('week')
    event_type = request.args.get('event_type')
    asset = request.args.get('asset')

    q = DriverEvent.query.filter(DriverEvent.Class=='Driver')
    if asset:
        q = q.filter(DriverEvent.AssetName==asset)
    elif driver_name:
        q = q.filter(DriverEvent.LinkedName_1==driver_name)
    if week:
        q = q.filter(func.to_char(DriverEvent.EventDate, 'IYYY-IW')==week)
    if event_type:
        q = q.filter(DriverEvent.EventTypes==event_type)

    events = q.order_by(DriverEvent.EventDate.desc()).limit(1000).all()
    score, det = compute_driver_score(events)
    return render_template('driver_events.html',
                           driver_name=driver_name or asset,
                           asset=asset,
                           events=events,
                           event_type=event_type,
                           driver_score=score,
                           score_details=det)
