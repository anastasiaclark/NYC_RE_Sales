--Create geometry and transform to NY State Plane Long Island (us-ft)

BEGIN TRANSACTION;

SELECT AddGeometryColumn ("yr_2019", "geometry", 2263, "POINT", "XY");

UPDATE yr_2019
SET geometry = Transform(MakePoint(long, lat, 4269),2263);

SELECT CreateSpatialIndex("yr_2019","geometry");

COMMIT;
