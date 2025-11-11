from flask import Blueprint, render_template, request, url_for
from models import VehicleEvent, db
from datetime import datetime, timedelta
from sqlalchemy import func, or_

vehicle_bp = Blueprint('vehicle', __name__)

# Define the battery disconnect alerts
BATTERY_DISCONNECT_ALERTS = [
    "Battery DC (A/H in depot)",
    "Battery DC (outside of depot)",
    "Battery Disconnect",
    "1A - Battery Disconnect",
    "1A - Battery Disconnect (Kathu)",
    "1A - Battery Disconnect (Kuruman)",
    "1E - Battery Disconnect"
]

def apply_filters(query, start_date, end_date, owner, asset_name, event_type):
    """Reusable filter function for table and chart queries."""
    if start_date:
        query = query.filter(VehicleEvent.EventDate >= datetime.strptime(start_date, "%Y-%m-%d"))
    if end_date:
        query = query.filter(VehicleEvent.EventDate <= datetime.strptime(end_date, "%Y-%m-%d"))
    if owner:
        query = query.filter(VehicleEvent.OwnerName.ilike(f"%{owner}%"))
    if asset_name:
        query = query.filter(VehicleEvent.AssetName == asset_name)
    if event_type:
        query = query.filter(VehicleEvent.EventTypes == event_type)
    return query

@vehicle_bp.route('/dashboard')
def vehicle_dashboard():
    # --- Filters ---
    start_date = request.args.get(
        'start_date', (datetime.utcnow() - timedelta(days=2)).strftime("%Y-%m-%d")
    )
    end_date = request.args.get('end_date', datetime.utcnow().strftime("%Y-%m-%d"))
    owner = request.args.get('owner')
    asset_name = request.args.get('asset_name')
    event_type = request.args.get('event_type')

    # --- Base table query ---
    table_query = VehicleEvent.query
    table_query = apply_filters(table_query, start_date, end_date, owner, asset_name, event_type)
    events = table_query.order_by(VehicleEvent.EventDate.desc()).limit(500).all()


    # --- Dropdowns ---
    # --- Dropdowns ---
    owners = [row[0] for row in db.session.query(VehicleEvent.OwnerName)
            .distinct().order_by(VehicleEvent.OwnerName).all()]

    # Filter assets based on selected owner
    if owner:
        assets = [row[0] for row in db.session.query(VehicleEvent.AssetName)
                .filter(VehicleEvent.OwnerName.ilike(f"%{owner}%"))
                .distinct().order_by(VehicleEvent.AssetName).all()]
    else:
        assets = [row[0] for row in db.session.query(VehicleEvent.AssetName)
                .distinct().order_by(VehicleEvent.AssetName).all()]

    event_types = [row[0] for row in db.session.query(VehicleEvent.EventTypes)
                .distinct().order_by(VehicleEvent.EventTypes).all()]


    # ================= Top 10 Drivers Table =================
    # ================= Top 10 Drivers Table =================
    top_driver_query = db.session.query(
        VehicleEvent.LinkedName_1,  # Driver names
        func.count(VehicleEvent.id).label("total_events")
    )

    # Apply filters first
    top_driver_query = apply_filters(top_driver_query, start_date, end_date, owner, asset_name, event_type)

    # Filter only Driver/Duty events, non-null LinkedName_1, and exclude Non Tagging events
    top_driver_query = top_driver_query.filter(
        VehicleEvent.LinkedName_1.isnot(None),
        VehicleEvent.Class.in_(["Driver", "Duty"]),
        VehicleEvent.EventTypes != "Non Tagging"
    )

    # Group by LinkedName_1 and get top 10
    top_drivers = (
        top_driver_query
        .group_by(VehicleEvent.LinkedName_1)
        .order_by(func.count(VehicleEvent.id).desc())
        .limit(10)
        .all()
    )

    vehicle_table = []
    for driver_name, total in top_drivers:
        # Breakdown per event type for this driver
        breakdown_query = db.session.query(
            VehicleEvent.EventTypes,
            func.count(VehicleEvent.id)
        ).filter(VehicleEvent.LinkedName_1 == driver_name)

        breakdown_query = apply_filters(breakdown_query, start_date, end_date, owner, None, event_type)
        breakdown_query = breakdown_query.filter(
            VehicleEvent.LinkedName_1.isnot(None),
            VehicleEvent.Class.in_(["Driver", "Duty"]),
            VehicleEvent.EventTypes != "Non Tagging"  # Exclude Non Tagging events
        )

        breakdown = breakdown_query.group_by(VehicleEvent.EventTypes).all()

        vehicle_table.append({
            "asset": driver_name,  # Show driver names
            "total": total,
            "breakdown": [{"type": etype, "count": cnt} for etype, cnt in breakdown]
        })


    # ================= Event Type Totals for Cards =================
    event_type_totals_query = db.session.query(
        VehicleEvent.EventTypes,
        func.count(VehicleEvent.id)
    )
    event_type_totals_query = apply_filters(event_type_totals_query, start_date, end_date, owner, asset_name, event_type)
    event_type_totals = event_type_totals_query.group_by(VehicleEvent.EventTypes).all()

    # Prepare drilldown data: for each Event Type, list of assets contributing to total
    event_type_drilldown = {}
    for etype, total in event_type_totals:
        asset_counts_query = db.session.query(
            VehicleEvent.AssetName,
            func.count(VehicleEvent.id)
        ).filter(VehicleEvent.EventTypes == etype)
        asset_counts_query = apply_filters(asset_counts_query, start_date, end_date, owner, asset_name, event_type)
        asset_counts = asset_counts_query.group_by(VehicleEvent.AssetName).order_by(func.count(VehicleEvent.id).desc()).all()
        
        # Only include assets with non-zero counts
        event_type_drilldown[etype] = [
            {"asset": asset, "count": cnt,
            "url": url_for('vehicle.vehicle_events', asset_name=asset, event_type=etype)}
            for asset, cnt in asset_counts if cnt > 0
        ]


    # ================= Existing Charts Logic =================
    # Weekly Asset Drilldown
    chart_query = db.session.query(VehicleEvent.AssetName, VehicleEvent.OwnerName, func.count(VehicleEvent.id))
    chart_query = apply_filters(chart_query, start_date, end_date, owner, asset_name, event_type)
    top_assets = chart_query.group_by(VehicleEvent.AssetName, VehicleEvent.OwnerName).order_by(func.count(VehicleEvent.id).desc()).limit(10).all()

    chart_data_weekly, drilldown_series_weekly = [], []
    for asset, owner_name, count in top_assets:
        weekly_query = db.session.query(
            func.to_char(VehicleEvent.EventDate, 'IYYY-IW').label('week'),
            func.count(VehicleEvent.id)
        ).filter(VehicleEvent.AssetName == asset, VehicleEvent.OwnerName == owner_name)
        weekly_query = apply_filters(weekly_query, start_date, end_date, owner, None, event_type)
        weekly_data = weekly_query.group_by('week').order_by('week').all()

        chart_data_weekly.append({
            'name': asset,
            'y': count,
            'drilldown': asset,
            'asset_name': asset,
            'owner': owner_name
        })

        week_drilldown = {'id': asset, 'name': f'Weekly events for {asset}', 'data': []}
        for week_str, week_count in weekly_data:
            week_drilldown['data'].append({
                'name': week_str,
                'y': week_count,
                'drilldown': f'{asset}_{week_str}',
                'asset_name': asset,
                'owner': owner_name
            })

            event_type_query = db.session.query(VehicleEvent.EventTypes, func.count(VehicleEvent.id))\
                .filter(VehicleEvent.AssetName == asset, VehicleEvent.OwnerName == owner_name, func.to_char(VehicleEvent.EventDate, 'IYYY-IW') == week_str)
            event_type_query = apply_filters(event_type_query, start_date, end_date, owner, None, event_type)
            event_type_counts = event_type_query.group_by(VehicleEvent.EventTypes).all()

            drilldown_series_weekly.extend([{
                'id': f'{asset}_{week_str}',
                'name': f'Event Types for {asset} - {week_str}',
                'data': [ {
                    'name': etype,
                    'y': etype_count,
                    'asset_name': asset,
                    'owner': owner_name,
                    'event_type': etype,
                    'url': url_for('vehicle.vehicle_events', asset_name=asset, week=week_str, event_type=etype)
                } for etype, etype_count in event_type_counts]
            }])

        drilldown_series_weekly.append(week_drilldown)

    # Hourly Stacked Column Chart
    hourly_query = db.session.query(
        func.extract('hour', VehicleEvent.EventDate).label('EventHour'),
        VehicleEvent.EventTypes.label('EventType'),
        VehicleEvent.AssetName,
        VehicleEvent.OwnerName,
        func.count(VehicleEvent.id).label('EventCount')
    )
    hourly_query = apply_filters(hourly_query, start_date, end_date, owner, asset_name, event_type)
    hourly_data_raw = [
        (int(hr), et.strip(), asset, owner_name, cnt)
        for hr, et, asset, owner_name, cnt in
        hourly_query.group_by('EventHour', VehicleEvent.EventTypes, VehicleEvent.AssetName, VehicleEvent.OwnerName).order_by('EventHour').all()
    ]

    hours = sorted({row[0] for row in hourly_data_raw})
    event_types_set = sorted({row[1] for row in hourly_data_raw})

    hourly_series, hour_drilldown_series = [], []
    for etype in event_types_set:
        hourly_series.append({
            'name': etype,
            'data': [{'y': sum(cnt for hr, et, asset, owner_name, cnt in hourly_data_raw if hr == hour and et == etype),
                      'drilldown': f'{hour}_{etype}'} for hour in hours]
        })

    for hour in hours:
        for etype in event_types_set:
            top_assets_query = db.session.query(
                VehicleEvent.AssetName,
                VehicleEvent.OwnerName,
                func.count(VehicleEvent.id).label('EventCount')
            ).filter(func.extract('hour', VehicleEvent.EventDate) == hour, VehicleEvent.EventTypes == etype)
            top_assets_query = apply_filters(top_assets_query, start_date, end_date, owner, asset_name, event_type)
            top_assets = [
                (asset.strip(), owner_name, cnt)
                for asset, owner_name, cnt in top_assets_query.group_by(VehicleEvent.AssetName, VehicleEvent.OwnerName).order_by(func.count(VehicleEvent.id).desc()).limit(10).all()
            ]

            hour_drilldown_series.append({
                'id': f'{hour}_{etype}',
                'name': f'Top Assets for Hour {hour} - {etype}',
                'data': [ {
                    'name': asset,
                    'y': count,
                    'asset_name': asset,
                    'owner': owner_name,
                    'event_type': etype
                } for asset, owner_name, count in top_assets]
            })

    # Owner -> EventType -> Top Assets Drilldown Pie Chart
    chart_data_owner, drilldown_series_owner = [], []
    owner_totals_query = db.session.query(VehicleEvent.OwnerName, func.count(VehicleEvent.id))
    owner_totals_query = apply_filters(owner_totals_query, start_date, end_date, owner, asset_name, event_type)
    owner_totals_query = owner_totals_query.group_by(VehicleEvent.OwnerName).all()

    for owner_name, owner_count in owner_totals_query:
        chart_data_owner.append({
            'name': owner_name,
            'y': owner_count,
            'drilldown': owner_name,
            'owner': owner_name
        })

        event_type_query = db.session.query(VehicleEvent.EventTypes, func.count(VehicleEvent.id))\
            .filter(VehicleEvent.OwnerName == owner_name)
        event_type_query = apply_filters(event_type_query, start_date, end_date, owner, asset_name, event_type)
        event_type_counts = event_type_query.group_by(VehicleEvent.EventTypes).all()

        et_data = []
        for etype, et_count in event_type_counts:
            top_assets_query = db.session.query(VehicleEvent.AssetName, func.count(VehicleEvent.id))\
                .filter(VehicleEvent.OwnerName == owner_name, VehicleEvent.EventTypes == etype)
            top_assets_query = apply_filters(top_assets_query, start_date, end_date, owner, asset_name, event_type)
            top_assets = top_assets_query.group_by(VehicleEvent.AssetName).order_by(func.count(VehicleEvent.id).desc()).limit(10).all()

            driver_data = [ {
                'name': asset,
                'y': count,
                'asset_name': asset,
                'owner': owner_name,
                'event_type': etype,
                'url': url_for('vehicle.vehicle_events', asset_name=asset, event_type=etype)
            } for asset, count in top_assets]

            drilldown_series_owner.append({
                'id': f'{owner_name}_{etype}',
                'name': f'Top Assets for {etype} in {owner_name}',
                'data': driver_data
            })

            et_data.append({
                'name': etype,
                'y': et_count,
                'drilldown': f'{owner_name}_{etype}'
            })

        drilldown_series_owner.append({
            'id': owner_name,
            'name': f'Event Types in {owner_name}',
            'data': et_data
        })

    # ================= Battery Disconnects Chart =================
    # Total battery disconnects
    battery_total_query = db.session.query(func.count(VehicleEvent.id))
    battery_total_query = battery_total_query.filter(VehicleEvent.AlertName.in_(BATTERY_DISCONNECT_ALERTS))
    battery_total_query = apply_filters(battery_total_query, start_date, end_date, owner, asset_name, event_type)
    total_battery_disconnects = battery_total_query.scalar() or 0

    battery_drilldown_query = db.session.query(
        VehicleEvent.AssetName,
        func.count(VehicleEvent.id)
    ).filter(VehicleEvent.AlertName.in_(BATTERY_DISCONNECT_ALERTS))
    battery_drilldown_query = apply_filters(battery_drilldown_query, start_date, end_date, owner, asset_name, event_type)
    battery_drilldown_query = battery_drilldown_query.group_by(VehicleEvent.AssetName).all()



    battery_chart_data = [{
        "name": "Battery Disconnects",
        "y": total_battery_disconnects,
        "drilldown": "battery_disconnects"
    }]

    battery_drilldown_series = [{
        "id": "battery_disconnects",
        "name": "Vehicles with Battery Disconnects",
        "data": [{"name": asset, "y": count, "asset_name": asset} for asset, count in battery_drilldown_query]
    }]

    return render_template(
        'vehicle.html',
        events=events,
        owners=owners,
        assets=assets,
        event_types=event_types,
        selected_owner=owner,
        selected_asset=asset_name,
        selected_event_type=event_type,
        start_date=start_date,
        end_date=end_date,
        vehicle_table=vehicle_table,
        event_type_totals=event_type_totals,
        event_type_drilldown=event_type_drilldown,  # <-- Add this
        chart_data_weekly=chart_data_weekly,
        drilldown_series_weekly=drilldown_series_weekly,
        hourly_series=hourly_series,
        hour_drilldown_series=hour_drilldown_series,
        chart_data_owner=chart_data_owner,
        drilldown_series_owner=drilldown_series_owner,
        battery_chart_data=battery_chart_data,
        battery_drilldown_series=battery_drilldown_series
    )


@vehicle_bp.route('/events/<asset_name>')
def vehicle_events(asset_name):
    week = request.args.get('week', None)
    event_type = request.args.get('event_type', None)

    # If asset_name is actually a driver, filter by LinkedName_1 instead of AssetName
    query = VehicleEvent.query.filter(
        (VehicleEvent.AssetName == asset_name) | (VehicleEvent.LinkedName_1 == asset_name)
    )

    # Only include driver/duty events if asset_name is a driver
    query = query.filter(
        (VehicleEvent.Class.in_(["Driver", "Duty"])) | (VehicleEvent.AssetName == asset_name)
    )

    if week:
        query = query.filter(func.to_char(VehicleEvent.EventDate, 'IYYY-IW') == week)
    if event_type:
        query = query.filter(VehicleEvent.EventTypes == event_type)

    events = query.order_by(VehicleEvent.EventDate.desc()).all()

    return render_template(
        'vehicle_events.html',
        asset_name=asset_name,
        events=events,
        event_type=event_type
    )



from flask import Blueprint, render_template, request
from models import VehicleEvent, DriverEvent, db
from sqlalchemy import and_, func, text
from sqlalchemy.orm import aliased

trip_bp = Blueprint('trip', __name__)

@trip_bp.route('/trip_events')
def trip_events():
    """
    Show vehicle and driver events linked to trips based on AssetName and EventDate.
    """
    # Filter params (optional)
    asset_name = request.args.get('asset_name')
    owner = request.args.get('owner')

    base_query = text("""
        SELECT 
            v.id AS vehicle_event_id,
            v."OwnerName",
            v."AssetName",
            v."EventDate",
            v."EventTypes",
            t.id AS trip_id,
            t.start,
            t."end",
            t.distance,
            t.start_coords,
            t.end_coords
        FROM vehicles v
        JOIN trips_data t
            ON v."AssetName" = t.asset
           AND v."EventDate" BETWEEN t.start::timestamp AND t."end"::timestamp
        {where_clause}
        ORDER BY v."EventDate" DESC
        LIMIT 500
    """)

    where_clauses = []
    params = {}
    if asset_name:
        where_clauses.append('AND v."AssetName" = :asset_name')
        params['asset_name'] = asset_name
    if owner:
        where_clauses.append('AND v."OwnerName" ILIKE :owner')
        params['owner'] = f"%{owner}%"

    final_query = base_query.bindparams(**params).columns().text.replace(
        "{where_clause}", "WHERE 1=1 " + " ".join(where_clauses)
    )

    results = db.session.execute(text(final_query), params).fetchall()

    return render_template("trip_events.html", events=results)
