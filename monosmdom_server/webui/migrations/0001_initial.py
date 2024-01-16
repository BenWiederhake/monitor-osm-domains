# Generated by Django 4.2.6 on 2024-01-16 20:37

from django.db import migrations, models


class Migration(migrations.Migration):
    initial = True

    dependencies = []

    operations = [
        migrations.CreateModel(
            name="DigestionHealth",
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
                ("digestion_begin", models.DateTimeField()),
                ("digestion_end", models.DateTimeField()),
                ("fresh_json", models.JSONField()),
                ("expensive_json", models.JSONField()),
            ],
        ),
    ]
