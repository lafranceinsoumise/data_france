# Generated by Django 3.1.7 on 2022-03-03 08:59

from django.db import migrations
import django_countries.fields


class Migration(migrations.Migration):

    dependencies = [
        ("data_france", "0032_ajouts_csp"),
    ]

    operations = [
        migrations.AddField(
            model_name="circonscriptionconsulaire",
            name="pays",
            field=django_countries.fields.CountryField(
                blank=True,
                max_length=746,
                multiple=True,
                verbose_name="Pays de la circonscription",
            ),
        ),
    ]