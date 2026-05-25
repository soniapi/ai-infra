// @generated automatically by Diesel CLI.

diesel::table! {
    objects (id) {
        id -> Int4,
        d -> Timestamp,
        t -> Text,
        p -> Float4,
        s -> Float4,
        c -> Float4,
    }
}

diesel::table! {
    objects_s (id, s) {
        id -> Int4,
        d -> Timestamp,
        t -> Text,
        p -> Float4,
        s -> Float4,
        c -> Float4,
    }
}

diesel::table! {
    objects_s_100000_above (id, s) {
        id -> Int4,
        d -> Timestamp,
        t -> Text,
        p -> Float4,
        s -> Float4,
        c -> Float4,
    }
}

diesel::table! {
    objects_s_100000_below (id, s) {
        id -> Int4,
        d -> Timestamp,
        t -> Text,
        p -> Float4,
        s -> Float4,
        c -> Float4,
    }
}

diesel::allow_tables_to_appear_in_same_query!(
    objects,
    objects_s,
    objects_s_100000_above,
    objects_s_100000_below,
);
