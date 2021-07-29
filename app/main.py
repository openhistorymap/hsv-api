from flask import Flask, Response, stream_with_context, jsonify
from flask import request, make_response
from flask_cors import CORS

import pyproj
from shapely.geometry import mapping, shape
from shapely.wkt import dumps
from shapely.ops import transform

from sqlalchemy import DDL, create_engine, MetaData, Table, Column, Integer, String, DateTime, Float, and_, or_, PrimaryKeyConstraint
from sqlalchemy.ext.declarative import declarative_base, instrument_declarative
from sqlalchemy.dialects.postgresql import BYTEA, JSONB
from sqlalchemy.orm import sessionmaker

from geoalchemy2 import Geometry
from geoalchemy2.functions import GenericFunction, ST_AsMVTGeom,  ST_TileEnvelope

import json
import os

POSTGRES = os.environ.get('POSTGRES', '51.15.160.236:25432')
POSTGRES_USER = os.environ.get('POSTGRES_USER', 'admin')
POSTGRES_PASS = os.environ.get('POSTGRES_PASS', 'tgZWW3Dgze94FN9O')
POSTGRES_DBNAME = os.environ.get('POSTGRES_DBNAME', 'ohm_hsv')

OHM_EXTENSIONS = """
create extension postgis;
create extension postgis_topology;
create extension plpgsql;
"""

import click

def create_app(): 
    app = Flask(__name__)
    
    engine = create_engine('postgresql://{}:{}@{}/{}'.format(
            POSTGRES_USER, POSTGRES_PASS, POSTGRES, POSTGRES_DBNAME
        ), pool_size=30, max_overflow=-1, pool_pre_ping=True,
        echo=False)
    Session = sessionmaker(bind=engine)
    db = engine.connect()

    @app.route('/')
    def index():
        return ''

    @app.route('/view', methods=['POST', 'PUT'])
    def add_view():
        return ''

    @app.route('/<year>/<z>/<y>/<x>/vector.pbf')
    def get_views(year, z, y, x):
        yr = float(year)
        iz = int(z)
        iy = int(y)
        ix = int(x)
        debug = request.args.get('debug')
        trange = request.args.get('range', '0')
        qs = []
        params = {'year': yr, 'z': iz, 'y': ix, 'x': iy}
        #k = "{layer}::{z}::{x}::{y}::{year}".format(**params)
        q = get_tile_for(params, True)
        if (q):
            tile = db.scalar(q)
            #cache.set(k, tile)
            x = b''.join([tile])
            return Response(x, mimetype='application/x-protobuf',)
        return ''


    def get_tile_for(params, debug):
        tq = "select ohm_hsv_tile({z}, {x}, {y}, {year})"
        q = tq.format(**params)    
        if debug:
            print(q)
        return q

    @app.route('/nearby.geojson', methods=['GET'])
    def get_nearby():
        lat = request.args.get('lat')
        lng = request.args.get('lng')
        ret = {}
        return Response(ret, mimetype='application/x-protobuf',)


    @app.route('/pic', methods=['POST'])
    def post_pic():
        session = Session()
        url = request.json.get('url')
        #author = request.args.get('author')

        d = Picture(url=url, author='ohm')
        session.add(d)
        session.commit()
        print(d.id)
        return Response(str(d.id))

    @app.route('/pic/<id>/meta', methods=['POST'])
    def post_meta(id):
        session = Session()
        additional = request.json.get('properties')
        timeline = request.json.get('timeline', 'default')
        ohm_from = request.json.get('ohm_from')
        ohm_to = request.json.get('ohm_to')
        lic = request.json.get('lic')
        coll = request.json.get('coll')
        dating = request.json.get('dating', 'period')
        desc = request.json.get('desc', '')
        notes = request.json.get('notes', '')
        media_type = request.json.get('media_type', 'photo')
        media_subtype = request.json.get('media_type', 'documentation')
        d = Metadata(
            pictureId = id,
            timeline = timeline,
            description = desc,
            dating = dating,
            license = lic,
            collection = coll,
            ohm_from = ohm_from,
            ohm_to = ohm_to,
            media_type = media_type,
            media_subtype = media_subtype,
            notes = notes,
            additional = additional,
            author = 'ohm',
        )
        session.add(d)
        session.commit()
        return Response(str(d.id))

    @app.route('/pic/<id>/loc', methods=['POST'])
    def post_loc(id):
        session = Session()
        geom = shape(request.json.get('geom'))
        geom_proj = request.json.get('proj', 'EPSG:4326')
        p_from = pyproj.CRS(geom_proj)
        p_to = pyproj.CRS('EPSG:3857')
        project = pyproj.Transformer.from_crs(p_from, p_to, always_xy=True).transform
        point = transform(project, geom)

        direction = request.json.get('dir', '0')
        angle = request.json.get('angle', '80')
        height = request.json.get('height', '1.6')

        d = Localization(
            pictureId = id,
            geom = dumps(point),
            direction = direction,
            angle = angle,
            height = height,
            author = "ohm"
        )
        session.add(d)
        session.commit()
        return Response(str(d.id))


    CORS(app, resources={r"*": {"origins": "*"}})
    return app

Base = declarative_base()

class Picture(Base):
    __tablename__ = 'pics'
    id = Column(Integer, autoincrement=True, nullable=False, primary_key=True)
    url = Column(String, index=True)
    author = Column(String, default='ohm')
    
class Localization(Base):
    __tablename__ = 'locs'
    id = Column(Integer, autoincrement=True, nullable=False, primary_key=True)
    pictureId = Column(Integer, index=True)
    geom = Column(Geometry(geometry_type='POINT', srid=3857, dimension=3))
    direction = Column(Integer)
    angle = Column(Integer)
    height = Column(Float)
    author = Column(String, default='ohm')
    
class Metadata(Base):
    __tablename__ = 'metas'
    id = Column(Integer, autoincrement=True, nullable=False, primary_key=True)
    pictureId = Column(Integer, index=True)
    timeline = Column(String, index=True, default='default')
    description = Column(String, index=True)
    dating = Column(String, index=True)
    license = Column(String, index=True)
    collection = Column(String, index=True, nullable=True)
    ohm_from = Column(Float, index=True)
    ohm_to = Column(Float, index=True)
    media_type = Column(String, index=True)
    media_subtype = Column(String, index=True)
    notes = Column(String, index=True)
    additional = Column(JSONB, index=True)
    author = Column(String, default='ohm')
    

OHM_PROCEDURE_MVT = """
DROP FUNCTION ohm_hsv_tile(integer,integer,integer,real);
create or replace function ohm_hsv_tile(
	z integer,
	x integer,
	y integer,
	t real
	) returns bytea
LANGUAGE plpgsql
AS $$
	 DECLARE 
	 tile bytea;
    BEGIN 
		select ST_AsMVT(q, 'pics', 4096, 'geom', 'fid') from (
                SELECT 
                    pics.id as fid,
                    pics.url,
					metas.*,
					locs.direction,
					locs.angle, 
					locs.height,
                    ST_AsMVTGeom(locs.geom, ST_TileEnvelope(z, x, y), 4096, 256, true) as geom
                FROM public.pics
					join locs on pics.id = locs."pictureId"
					join metas on pics.id = metas."pictureId"
                where 
                    locs.geom && ST_TileEnvelope(z, x, y) AND
                    metas.ohm_from <= t AND ( metas.ohm_to > t OR metas.ohm_to is Null) 
            ) as q into tile;
		return tile;
    end;
$$; 
"""

OHM_PROCEDURE_GEOJ = """
DROP FUNCTION ohm_hsv_geoj(point)
create or replace function ohm_hsv_tile(
	point POINT,
	) returns string
LANGUAGE plpgsql
AS $$
	 DECLARE 
	 tile bytea;
    BEGIN 
		select ST_AsMVT(q, 'pics', 4096, 'geom', 'fid') from (
                SELECT 
                    pics.id as fid, 
                    metas.*, 
                    ST_AsMVTGeom(geometries.geom, ST_TileEnvelope(z, x, y), 4096, 256, true) as geom
                FROM public.pics
					join locs on pics.id = locs."pictureId"
					join metas on pics.id = metas."pictureId"
                where 
                    locs.geom && ST_TileEnvelope(z, x, y) AND
                    metas.ohm_from <= t AND ( metas.ohm_to > t OR metas.ohm_to is Null) 
            ) as q into tile;
		return tile;
    end;
$$; 
"""

@click.group()
def cli():
    pass

@click.command()
@click.option('--dbname', default=POSTGRES_DBNAME, help='database name')
def initdb(dbname):
    engine = create_engine('postgresql://{}:{}@{}/{}'.format(
            POSTGRES_USER, POSTGRES_PASS, POSTGRES, dbname
        ), pool_size=20, max_overflow=0, pool_pre_ping=True,
        echo=False)
    db = engine.connect()
    for ex in OHM_EXTENSIONS.split('\n'):
        if len(ex) > 0:
            try:
                db.execute(ex)
            except:
                pass
    Base.metadata.create_all(engine)
    try:
        db.execute(OHM_PREPARE_CONF__DROP)
    except:
        pass
    
    db.execute(OHM_PROCEDURE_MVT)
    click.echo('Initialized the database')


@click.command()
def run():
    app = create_app()
    app.run(host='0.0.0.0', port='9055', debug=True, threaded=True)

cli.add_command(initdb)
cli.add_command(run)

if __name__ == '__main__':
    cli()