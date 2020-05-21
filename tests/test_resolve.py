import json
import pytest

from sno.merge_util import MergedOursTheirs, MergeIndex

H = pytest.helpers.helpers()


def test_resolve_conflicts(create_conflicts, cli_runner):
    with create_conflicts(H.POLYGONS) as repo:
        r = cli_runner.invoke(["merge", "theirs_branch", "--json"])
        assert r.exit_code == 0, r
        assert json.loads(r.stdout)["sno.merge/v1"]["conflicts"]

        def get_conflict_ids(cli_runner):
            r = cli_runner.invoke(["conflicts", "-s", "--flat", "--json"])
            assert r.exit_code == 0, r
            return json.loads(r.stdout)["sno.conflicts/v1"]

        conflict_ids = get_conflict_ids(cli_runner)
        resolutions = iter(["--ancestor", "--ours", "--theirs", "--delete"])

        # Keep track of which order we resolve the conflicts - internally, these
        # conflicts have keys "0", "1", "2", "3" with an ordering based on their
        # repo paths, but `sno conflicts` shows them in a different order based
        # on their primary keys.
        key_order = []

        while conflict_ids:
            num_conflicts = len(conflict_ids)
            cli_runner.invoke(["resolve", conflict_ids[0], next(resolutions)])
            assert r.exit_code == 0, r
            conflict_ids = get_conflict_ids(cli_runner)
            assert len(conflict_ids) == num_conflicts - 1

            resolved_keys = MergeIndex.read_from_repo(repo).resolves.keys()
            key_order += [k for k in resolved_keys if k not in key_order]

        assert len(conflict_ids) == 0

        merge_index = MergeIndex.read_from_repo(repo)
        assert len(merge_index.entries) == 242
        assert len(merge_index.conflicts) == 4
        assert len(merge_index.resolves) == 4

        k0, k1, k2, k3 = key_order
        assert merge_index.resolves[k0].merged == merge_index.conflicts[k0].ancestor
        assert merge_index.resolves[k1].merged == merge_index.conflicts[k1].ours
        assert merge_index.resolves[k2].merged == merge_index.conflicts[k2].theirs
        assert merge_index.resolves[k3] == MergedOursTheirs.EMPTY
