# Copyright (C) 2021 Bosutech XXI S.L.
#
# nucliadb is offered under the AGPL v3.0 and as commercial software.
# For commercial licensing, contact us at info@nuclia.com.
#
# AGPL:
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
import argparse
import logging
import traceback
import uuid
from typing import Dict, List, Optional

from kb_admin import KBNotFoundError, KnowledgeBoxAdmin, VectorsRecomputer, logger
from pydantic import BaseModel

INGEST_GRPC_PORT = 8030
API_HTTP_PORT = 8080
TRAIN_GRPC_PORT = 8080
INTERNAL_SERVICES = {
    "writer": "writer.nucliadb.svc.cluster.local",
    "search": "search.nucliadb.svc.cluster.local",
    "reader": "reader.nucliadb.svc.cluster.local",
    "train": "train.nucliadb.svc.cluster.local",
    "ingest": "ingest.nucliadb.svc.cluster.local",
    "learning": "supervisor-ml.learning.svc.cluster.local",
}
DEFAULT_BATCH_SIZE = 10

import json
import pathlib

SESSIONS_PATH = pathlib.Path(".sessions")
if not SESSIONS_PATH.exists():
    SESSIONS_PATH.mkdir()

CURRENT_KB = None


class StepCheckpoint(BaseModel):
    kb: str
    step_id: str
    done: List[str] = []


class FixSession(BaseModel):
    id: str
    vectors: List[str] = []
    cleanup: List[str] = []
    reindex: List[str] = []

    ongoing_kbs: Dict[str, StepCheckpoint] = {}

    @classmethod
    def load(kls, id: str):
        session_file = SESSIONS_PATH / id
        with open(session_file, "r") as f:
            return kls.parse_obj(json.loads(f.read()))

    def save(self):
        session_file = SESSIONS_PATH / self.id
        with open(session_file, "w") as f:
            f.write(json.dumps(self.dict()))

    def get_step_checkpoint(self, kb: str) -> Optional[StepCheckpoint]:
        return self.ongoing_kbs.get(kb)

    def set_step_checkpoint(self, kb: str, step: StepCheckpoint):
        self.ongoing_kbs[kb] = step

    def mark_step_done(self, kb, step):
        if step == "reindex":
            self.reindex.append(kb)
        elif step == "vectors":
            self.vectors.append(kb)
        elif step == "cleanup":
            self.cleanup.append(kb)
        else:
            raise ValueError(step)
        self.ongoing_kbs.pop(kb, None)
        self.save()

    def is_kb_fixed(self, kb: str) -> bool:
        return (
            self.is_step_fixed(kb, "vectors")
            and self.is_step_fixed(kb, "cleanup")
            and self.is_step_fixed(kb, "reindex")
        )

    def is_step_fixed(self, kb: str, step: str) -> bool:
        if step not in ("vectors", "cleanup", "reindex"):
            raise ValueError()
        return kb in getattr(self, step)


def fix_it(
    kbadmin: KnowledgeBoxAdmin,
    vr: VectorsRecomputer,
    steps=["all"],
    session=FixSession(id="noop"),
    force=False,
):
    slug = kbadmin.kb.slug  # type: ignore

    all_steps = "all" in steps
    vectors_step = "vectors" in steps or all_steps
    cleanup_step = "cleanup" in steps or all_steps
    reindex_step = "reindex" in steps or all_steps

    if vectors_step:
        if not force and session.is_step_fixed(slug, "vectors"):
            logger.debug("Skipping recomputing vectors. Already done in this session!")
        else:
            checkpoint = session.get_step_checkpoint(slug)
            if checkpoint is None or checkpoint.step_id != "vectors":
                checkpoint = StepCheckpoint(kb=slug, step_id="vectors")
                session.set_step_checkpoint(slug, checkpoint)

            kbadmin.recompute_vectors(vr, computed_fields=checkpoint.done)
            if not kbadmin.dry_run:
                session.mark_step_done(slug, "vectors")

    if cleanup_step:
        if not force and session.is_step_fixed(slug, "cleanup"):
            logger.debug("Skipping index clean and upgrade")
        else:
            kbadmin.clean_index()
            if not kbadmin.dry_run:
                session.mark_step_done(slug, "cleanup")

    if reindex_step:
        if not force and session.is_step_fixed(slug, "reindex"):
            logger.debug("Skipping reindex. Already done in this session!")
        else:
            checkpoint = session.get_step_checkpoint(kb=slug)
            if checkpoint is None or checkpoint.step_id != "reindex":
                checkpoint = StepCheckpoint(kb=slug, step_id="reindex")
                session.set_step_checkpoint(slug, checkpoint)

            kbadmin.reindex(reindexed=checkpoint.done)
            if not kbadmin.dry_run:
                session.mark_step_done(slug, "reindex")


def load_or_create_session(session_id: str) -> FixSession:
    logger.info(f"Picking up from a previous session: {args.session}")
    try:
        session = FixSession.load(session_id)
    except FileNotFoundError:
        logger.info(
            f"Existing session not found. Creating new one with id: {session_id}"
        )
        session = FixSession(id=session_id)
    return session


class KBLoggingFilter(logging.Filter):
    def filter(self, record):
        kb = get_current_kb()
        if kb is None:
            record.slug = ""
        else:
            record.slug = kb.slug
        return True


def get_current_kb():
    global CURRENT_KB

    return CURRENT_KB


def set_current_kb(kb):
    global CURRENT_KB

    CURRENT_KB = kb


def configure_logging(session_id: str, debug=False):
    level = logging.INFO
    if debug is True:
        level = logging.DEBUG

    fmt = logging.Formatter(
        "[%(asctime)s][%(levelname)s][%(slug)s] %(message)s",
        datefmt="%m/%d/%Y %I:%M:%S %p",
    )
    fh = logging.FileHandler("{0}/{1}.log".format(SESSIONS_PATH, session_id))
    fh.setLevel(level)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    sh = logging.StreamHandler()
    sh.setLevel(level)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    kbf = KBLoggingFilter()
    logger.addFilter(kbf)
    logger.setLevel(level)


def parse_arguments():
    parser = argparse.ArgumentParser(description="Script to fix production indexes")
    parser.add_argument("-b", "--kb-batch", type=int, default=10)
    parser.add_argument(
        "--session",
        dest="session",
        default=uuid.uuid4().hex[:8],
        help="Session id. Use to continue work from previous session",
    )
    parser.add_argument(
        "--kb",
        dest="kb",
        required=False,
        help="KB slug or uuid. Use to fix a particular KB",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument(
        "-c",
        "--continue-on-error",
        action="store_true",
        help="Set if you want to move on to other KBs if there is an error",
    )
    parser.add_argument(
        "--step",
        nargs="+",
        default=["all"],
        choices=["all", "vectors", "cleanup", "reindex"],
    )
    args = parser.parse_args()
    return args


def main(args):
    configure_logging(args.session, debug=args.debug)
    session = load_or_create_session(args.session)
    vr = VectorsRecomputer()
    kbadmin = KnowledgeBoxAdmin(
        host=INTERNAL_SERVICES["ingest"],
        grpc=INGEST_GRPC_PORT,
        http=API_HTTP_PORT,
        train_port=TRAIN_GRPC_PORT,
        reader_host=INTERNAL_SERVICES["reader"],
        writer_host=INTERNAL_SERVICES["writer"],
        search_host=INTERNAL_SERVICES["search"],
        train_host=INTERNAL_SERVICES["train"],
        grpc_host=INTERNAL_SERVICES["ingest"],
        dry_run=args.dry_run,
        learning_grpc=f"{INTERNAL_SERVICES['learning']}:8090",
    )
    if args.kb:
        try:
            kb = kbadmin.set_kb(args.kb)
        except KBNotFoundError:
            logger.warning(f"KB not found!")
            return

        set_current_kb(kb)
        fix_it(kbadmin, vr, steps=args.step, session=session, force=True)

    else:
        all_kbs = kbadmin.client.list_kbs(timeout=10)
        all_kbs.sort(key=lambda x: x.slug)
        total_kbs = len(all_kbs)
        logger.info(f"Found {total_kbs} kbs!")
        fixed_in_session = 0
        for index, kb in enumerate(all_kbs):
            slug = kb.slug
            try:
                try:
                    kb = kbadmin.set_kb(slug)
                except KBNotFoundError:
                    logger.warning(f"Not found kb={slug}. Moving on...")
                    continue

                if fixed_in_session >= args.kb_batch:
                    logger.info(
                        f"Batch finished! Use --session={session.id} to continue"
                    )
                    break

                set_current_kb(kb)

                if session.is_kb_fixed(slug):
                    logger.debug(f"Skipping. Already fixed")
                    continue

                percent = int(index * 100 / total_kbs)
                logger.info(f"Fixing {index}-th kb of {total_kbs} ({percent}%)")
                fix_it(kbadmin, vr, steps=args.step, session=session)

            except Exception:
                logger.error(f"Error fixing kbid={kb.kbid}")
                traceback.print_exc()
                if not args.continue_on_error:
                    raise

            finally:
                session.save()
                fixed_in_session += 1

        if index + 1 >= len(all_kbs):
            logger.info("Finished fixing all kbs!")


if __name__ == "__main__":
    args = parse_arguments()
    main(args)
