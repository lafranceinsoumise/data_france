# Generated by Django 3.1.7 on 2021-07-06 09:53

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('data_france', '0022_auto_20210528_0804'),
    ]

    operations = [
        migrations.AlterField(
            model_name='commune',
            name='type',
            field=models.CharField(choices=[('COM', 'Commune'), ('COMD', 'Commune déléguée'), ('COMA', 'Commune associée'), ('ARM', 'Arrondissement PLM'), ('SRM', 'Secteur électoral PLM')], default='COM', editable=False, max_length=4, verbose_name='Type de commune'),
        ),
    ]