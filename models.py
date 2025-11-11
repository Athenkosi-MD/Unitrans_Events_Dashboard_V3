from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()

# Common base class with shared columns
class BaseEvent(db.Model):
    __abstract__ = True

    OwnerName = db.Column(db.String, index=True)
    AlertId = db.Column(db.String)
    AlertName = db.Column(db.String)
    AlertType = db.Column(db.String)
    EventDate = db.Column(db.DateTime, index=True)
    CreationDate = db.Column(db.DateTime)
    ModifiedDate = db.Column(db.DateTime)
    EventClass = db.Column(db.String)
    EventType = db.Column(db.String)
    LinkedId_0 = db.Column(db.String)
    LinkedName_0 = db.Column(db.String)
    LinkedId_1 = db.Column(db.String, index=True)
    LinkedName_1 = db.Column(db.String, index=True)
    Longitude = db.Column(db.Float)
    Latitude = db.Column(db.Float)
    LocationAddress = db.Column(db.String)
    AssetId = db.Column(db.String, index=True)
    AssetName = db.Column(db.String, index=True)
    AssetTypeId = db.Column(db.String)
    AssetTypeName = db.Column(db.String)
    InputId = db.Column(db.String)
    InputName = db.Column(db.String)
    LimitValue = db.Column(db.Float)
    CurrentValue = db.Column(db.Float)
    IdleCounter = db.Column(db.Float)
    BatteryVoltage = db.Column(db.Float)
    PowerVoltage = db.Column(db.Float)
    EventTypes = db.Column("Event Types", db.String, index=True)
    AlertClassification = db.Column("Alert Classification", db.String)
    Class = db.Column(db.String, index=True)


class DriverEvent(BaseEvent):
    __tablename__ = 'drivers'
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)


class VehicleEvent(BaseEvent):
    __tablename__ = 'vehicles'
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
