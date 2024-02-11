# Generated by Django 4.2.6 on 2024-02-11 15:01

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    dependencies = [
        ("crawl", "0006_mark_error_type"),
    ]

    operations = [
        migrations.CreateModel(
            name="SquatProof",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("squatter", models.TextField()),
                (
                    "evidence",
                    models.OneToOneField(
                        on_delete=django.db.models.deletion.RESTRICT,
                        to="crawl.resultsuccess",
                    ),
                ),
            ],
        ),
    ]
