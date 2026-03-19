use diesel::prelude::*;
use chrono::NaiveDateTime;

 use crate::schema::objects;
 use crate::schema::objects_s;

#[derive(Queryable, Selectable, Debug)]
#[diesel(table_name = objects)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct Object {
    pub id: i32,
    pub d: NaiveDateTime,
    pub t: String,
    pub p: f32,
    pub s: f32,
    pub c: f32,
}

#[derive(Insertable, Debug)]
#[diesel(table_name = objects)]
pub struct NewObject<'a> {
    pub d: &'a NaiveDateTime,
    pub t: &'a String,
    pub p: &'a f32,
    pub s: &'a f32,
    pub c: &'a f32,
}

#[derive(Queryable, Selectable, Debug)]
#[diesel(table_name = objects_s)] // partioned table by s
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct ObjectS {
    pub id: i32,
    pub d: NaiveDateTime,
    pub t: String,
    pub p: f32,
    pub s: f32,
    pub c: f32,
}

#[derive(Insertable, Debug)]
#[diesel(table_name = objects_s)]
pub struct NewObjectS<'a> {
    pub d: &'a NaiveDateTime,
    pub t: &'a String,
    pub p: &'a f32,
    pub s: &'a f32,
    pub c: &'a f32,
}

