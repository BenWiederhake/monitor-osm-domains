# Generated by Django 4.2.6 on 2023-10-20 00:12

from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("storage", "0001_initial"),
    ]

    operations = [
        migrations.RemoveField(
            model_name="domain",
            name="is_ignored",
        ),
        migrations.RemoveField(
            model_name="url",
            name="in_osm_data",
        ),
    ]
