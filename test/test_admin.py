from django.contrib.auth.models import User
from django.db.models import Q
from django.test import TestCase
from django.urls import reverse

from data_france.models import (
    Commune,
    EPCI,
    Departement,
    Region,
    CollectiviteDepartementale,
    CollectiviteRegionale,
    Senateur,
)


class AdminTestCase(TestCase):
    def setUp(self) -> None:
        user = User.objects.create_superuser("test")
        self.client.force_login(user)

    def test_admin_communes(self):
        res = self.client.get(reverse("admin:data_france_commune_changelist"))
        self.assertEqual(200, res.status_code)

        c = Commune.objects.filter(type="COM").order_by("?").first()

        res = self.client.get(reverse("admin:data_france_commune_change", args=(c.id,)))
        self.assertEqual(200, res.status_code)

    def test_admin_epci(self):
        res = self.client.get(reverse("admin:data_france_epci_changelist"))
        self.assertEqual(200, res.status_code)

        c = EPCI.objects.order_by("?").first()

        res = self.client.get(reverse("admin:data_france_epci_change", args=(c.id,)))
        self.assertEqual(200, res.status_code)

    def test_admin_departement(self):
        res = self.client.get(reverse("admin:data_france_departement_changelist"))
        self.assertEqual(200, res.status_code)

        c = Departement.objects.order_by("?").first()

        res = self.client.get(
            reverse("admin:data_france_departement_change", args=(c.id,))
        )
        self.assertEqual(200, res.status_code)

    def test_admin_region(self):
        res = self.client.get(reverse("admin:data_france_region_changelist"))
        self.assertEqual(200, res.status_code)

        c = Region.objects.order_by("?").first()

        res = self.client.get(reverse("admin:data_france_region_change", args=(c.id,)))
        self.assertEqual(200, res.status_code)

    def test_admin_collectivite_departementale(self):
        res = self.client.get(
            reverse("admin:data_france_collectivitedepartementale_changelist")
        )
        self.assertEqual(200, res.status_code)

        c = CollectiviteDepartementale.objects.order_by("?").first()

        res = self.client.get(
            reverse("admin:data_france_collectivitedepartementale_change", args=(c.id,))
        )
        self.assertEqual(200, res.status_code)

    def test_admin_collectivite_regionale(self):
        res = self.client.get(
            reverse("admin:data_france_collectiviteregionale_changelist")
        )
        self.assertEqual(200, res.status_code)

        c = CollectiviteRegionale.objects.order_by("?").first()

        res = self.client.get(
            reverse("admin:data_france_collectiviteregionale_change", args=(c.id,))
        )
        self.assertEqual(200, res.status_code)

    def test_admin_senateur(self):
        res = self.client.get(reverse("admin:data_france_senateur_changelist"))
        self.assertEqual(200, res.status_code)

        # avec et sans département
        for s in [
            Senateur.objects.exclude(departement=None).order_by("?").first(),
            Senateur.objects.filter(departement=None).order_by("?").first(),
        ]:
            res = self.client.get(
                reverse("admin:data_france_senateur_change", args=(s.id,))
            )
            self.assertEqual(200, res.status_code)

    def test_admin_senateur_filtre_groupe(self):
        url = reverse("admin:data_france_senateur_changelist")

        res = self.client.get(url, {"groupe": "NI"})
        self.assertEqual(200, res.status_code)
        self.assertEqual(
            res.context["cl"].result_count,
            Senateur.objects.filter(
                Q(groupe="") | Q(groupe__endswith="(NI)")
            ).count(),
        )

        s = (
            Senateur.objects.exclude(groupe="")
            .exclude(groupe__endswith="(NI)")
            .order_by("?")
            .first()
        )
        if s is None:
            self.skipTest("Aucun sénateur membre d'un groupe")

        res = self.client.get(url, {"groupe": s.groupe})
        self.assertEqual(200, res.status_code)
        self.assertEqual(
            res.context["cl"].result_count,
            Senateur.objects.filter(groupe=s.groupe).count(),
        )
