from django.db import connection, transaction
from django.utils import timezone
import collections
import contextlib
import datetime


def now_tzaware():
    return timezone.localtime(timezone.now())


PropagationDestination = collections.namedtuple("PropagationDestination", ["clazz", "index_in_list", "field_name"])
DeferEntry = collections.namedtuple("DeferEntry", ["obj", "propagate_to"])


@contextlib.contextmanager
def deferred_upserts():
    # We need the IDs of inserted rows, which are only generated if we're not inside a
    # transaction.atomic() block. Assert this by pretending to start an outermost transaction:
    with transaction.atomic(durable=True):
        pass
    objects_by_class = collections.defaultdict(list)  # class to list of DeferEntry
    propagations = collections.defaultdict(list)  # class to list of objects
    class_order = list()  # list with properly-resolved order of upsert model classes.

    def defer(obj, *, _magically_propagate_to=None):
        """
        Upserts ("get_or_create") an object of an arbitrary model, but not right away. The upsert is
        only executed as part of trigger(), which eventually calls ModelManager.bulk_create().

        In order to support this, a Model must define three lists as class properties:
        - upsert_update_fields: List of strings that indicate fields that shall be written to the
          DB. Passed as-is to QuerySet.bulk_create. See also:
          https://docs.djangoproject.com/en/4.1/ref/models/querysets/#django.db.models.query.QuerySet.bulk_create
        - upsert_unique_fields: List of strings that indicate fields that shall be used to detect
          updates (instead of insertions). Passed as-is to QuerySet.bulk_create. See above.
        - upsert_foreign_fields: List of strings that indicate fields with foreign fields. These
          foreign keys must not be None in any of the passed objects. This list must EXACTLY the
          same as:
            set(upsert_unique_fields + upsert_update_fields).intersect(all_foreignkey_fields_of_the_model)
          TODO: Assert correctness of upsert_foreign_fields programmatically.
          This is necessary to ensure that the bulk-insert operations happen in the correct order.
        """
        assert isinstance(type(obj).upsert_update_fields, list), f"Type {type(obj)} not prepared for bulk upserts!"
        assert isinstance(type(obj).upsert_unique_fields, list), f"Type {type(obj)} not prepared for bulk upserts!"
        assert isinstance(type(obj).upsert_foreign_fields, list), f"Type {type(obj)} not prepared for bulk upserts!"
        # Ugly hack: We must determine how many objects already exist before us, which might create
        # a new entry in objects_by_class, so we absolutely must use `class_order` to store the
        # correct insertion order. Ugh!
        own_index_in_list = len(objects_by_class[type(obj)])
        for foreign_field in type(obj).upsert_foreign_fields:
            dest = PropagationDestination(type(obj), own_index_in_list, foreign_field)
            # Make absolutely sure that foreign keys are resolvable first:
            defer(getattr(obj, foreign_field), _magically_propagate_to=dest)
        if type(obj) not in class_order:
            # TODO: Try to assert the thing about foreign key models?
            class_order.append(type(obj))
        objects_by_class[type(obj)].append(DeferEntry(obj, _magically_propagate_to))
        assert len(objects_by_class[type(obj)]) == own_index_in_list + 1, "Some kind of cycle?!"

    # Context-switch to the caller, enabling them to defer upserts with this callback:
    yield defer

    # Run *all* the deferred bulk updates:
    for clazz in class_order:
        objects_in_class = objects_by_class[clazz]
        if not objects_in_class:
            continue
        print(f"    bulk inserting {len(objects_in_class)} instances of {clazz}, through {connection.vendor}")
        objects_with_ids = clazz.objects.bulk_create(
            [e.obj for e in objects_in_class],
            # Django refuses to do upserts that can never update anything due to empty
            # unique_fields. I guess that makes sense, but is annoying here:
            update_conflicts=bool(clazz.upsert_unique_fields),
            update_fields=clazz.upsert_update_fields,
            unique_fields=clazz.upsert_unique_fields,
        )
        print(f"    -> {objects_with_ids=}")  # FIXME: IDs are missing?!?!?!
        # Sadly, bulk_create does not update primary keys in Model instances, but rather returns
        # completely new instances. This is probably because in the case of a pure-update, semantics
        # aren't too clear otherwise. So we need to check whether this was supposed to propagate:
        assert len(objects_in_class) == len(objects_with_ids)
        for old_entry, obj_with_id in zip(objects_in_class, objects_with_ids):
            if old_entry.propagate_to is None:
                continue
            prop_dest = old_entry.propagate_to
            prop_dest_obj = objects_by_class[prop_dest.clazz][prop_dest.index_in_list].obj
            print(f"{prop_dest=}, specifically writing {obj_with_id=} into {prop_dest_obj=}")
            setattr(prop_dest_obj, prop_dest.field_name, obj_with_id)
