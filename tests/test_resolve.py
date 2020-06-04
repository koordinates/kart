import json
import pytest

from sno.diff_output import json_row
from sno.exceptions import INVALID_OPERATION
from sno.merge_util import MergeIndex
from sno.structure import RepositoryStructure

H = pytest.helpers.helpers()


def test_resolve_conflicts(create_conflicts, cli_runner):
    with create_conflicts(H.POLYGONS) as repo:
        r = cli_runner.invoke(["merge", "--continue"])
        assert r.exit_code == INVALID_OPERATION

        r = cli_runner.invoke(["merge", "theirs_branch", "--json"])
        assert r.exit_code == 0, r
        assert json.loads(r.stdout)["sno.merge/v1"]["conflicts"]

        def get_conflict_ids(cli_runner):
            r = cli_runner.invoke(["conflicts", "-s", "--flat", "--json"])
            assert r.exit_code == 0, r
            return json.loads(r.stdout)["sno.conflicts/v1"]

        conflict_ids = get_conflict_ids(cli_runner)
        resolutions = iter(["--ancestor", "--ours", "--theirs", "--delete"])

        # Keep track of which order we resolve the conflicts - each conflict
        # resolved will have a primary key, and we resolve conflicts in
        # primary key order, but the primary keys are not contiguous.
        pk_order = []
        # Each conflict also has an internal "conflict" key - just its index
        # in the original list of conflicts - these are contiguous, but
        # we don't necessarily resolve the conflicts in this order.
        ck_order = []

        while conflict_ids:
            num_conflicts = len(conflict_ids)
            conflict_id = conflict_ids[0]
            pk = conflict_id.split("=", 1)[1]
            pk_order += [pk]

            r = cli_runner.invoke(["resolve", conflict_id, next(resolutions)])
            assert r.exit_code == 0, r
            conflict_ids = get_conflict_ids(cli_runner)
            assert len(conflict_ids) == num_conflicts - 1

            resolved_keys = MergeIndex.read_from_repo(repo).resolves.keys()
            ck_order += [k for k in resolved_keys if k not in ck_order]

        assert len(conflict_ids) == 0

        merge_index = MergeIndex.read_from_repo(repo)
        assert len(merge_index.entries) == 242
        assert len(merge_index.conflicts) == 4
        assert len(merge_index.resolves) == 4

        ck0, ck1, ck2, ck3 = ck_order
        # Conflict ck0 is resolved to ancestor, but the ancestor is None.
        assert merge_index.resolves[ck0] == []
        assert merge_index.conflicts[ck0].ancestor is None
        assert merge_index.resolves[ck1] == [merge_index.conflicts[ck1].ours]
        assert merge_index.resolves[ck2] == [merge_index.conflicts[ck2].theirs]
        assert merge_index.resolves[ck3] == []

        r = cli_runner.invoke(["merge", "--continue"])

        merged = RepositoryStructure.lookup(repo, "HEAD")
        ours = RepositoryStructure.lookup(repo, "ours_branch")
        theirs = RepositoryStructure.lookup(repo, "theirs_branch")

        def feature_to_json(rs, pk):
            _, feature = rs[H.POLYGONS.LAYER].get_feature(pk, ogr_geoms=False)
            return json_row(feature, H.POLYGONS.LAYER_PK)

        def assert_no_such_feature(rs, pk):
            with pytest.raises(KeyError):
                rs[H.POLYGONS.LAYER].get_feature(pk, ogr_geoms=False)

        pk0, pk1, pk2, pk3 = pk_order
        assert_no_such_feature(merged, pk0)  # Resolved to ancestor, which was None.
        assert feature_to_json(merged, pk1) == feature_to_json(ours, pk1)
        assert feature_to_json(merged, pk2) == feature_to_json(theirs, pk2)
        assert_no_such_feature(merged, pk3)
