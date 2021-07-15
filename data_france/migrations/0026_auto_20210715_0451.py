# Generated by Django 3.1.7 on 2021-07-15 04:51

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('data_france', '0025_auto_20210707_0901'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='collectivitedepartementale',
            name='departement',
        ),
        migrations.AddField(
            model_name='collectivitedepartementale',
            name='region',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.PROTECT, to='data_france.region', verbose_name='Région'),
        ),
        migrations.AlterField(
            model_name='collectivitedepartementale',
            name='type',
            field=models.CharField(choices=[('D', 'Conseil départemental'), ('S', 'Collectivité à statut particulier')], max_length=1, verbose_name='Type de collectivité départementale'),
        ),
        migrations.AlterField(
            model_name='collectiviteregionale',
            name='type',
            field=models.CharField(choices=[('R', 'Conseil régional'), ('U', 'Collectivité territoriale unique')], max_length=1, verbose_name='Type de collectivité'),
        ),
    ]