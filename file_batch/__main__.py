import argparse
import json
import os
import sys
from .bundle import BatchError
from .compare import compare
from .export import export_bundle
from .runner import run
from .store import Store
from .verify import verify as verify_paths


def main():
    parser=argparse.ArgumentParser(description='파일 기반 일 정산 (DSN은 환경변수로 주입)')
    commands=parser.add_subparsers(dest='command',required=True)
    commands.add_parser('init')
    export=commands.add_parser('export')
    for arg in ('date','root','masters','close'):
        export.add_argument('--'+arg,required=True)
    execute=commands.add_parser('run')
    execute.add_argument('--bundle',required=True)
    execute.add_argument('--work-root',default='data/file-batch/runs')
    execute.add_argument('--expected-revision',type=int,default=0)
    verify=commands.add_parser('compare')
    for arg in ('legacy-db','file-db','date','report'):
        verify.add_argument('--'+arg,required=True)
    both=commands.add_parser('verify')
    both.add_argument('--bundle',required=True)
    both.add_argument('--landing',required=True)
    both.add_argument('--work-root',default='data/file-batch/verification')
    for name in ('finalize','reopen'):
        cmd=commands.add_parser(name)
        cmd.add_argument('--date',required=True)
        cmd.add_argument('--revision',required=True,type=int)
        cmd.add_argument('--reason',required=True)
    args=parser.parse_args()
    try:
        if args.command=='init':
            Store(os.environ['FILE_BATCH_SERVING_DSN']).init()
            result={'status':'initialized'}
        elif args.command=='export':
            result={'bundle':str(export_bundle(os.environ['FILE_BATCH_PAY_DSN'],os.environ['FILE_BATCH_ORDER_DSN'],
                         args.date,args.root,args.masters,args.close))}
        elif args.command=='run':
            result=run(args.bundle,args.work_root,os.environ['FILE_BATCH_SERVING_DSN'],args.expected_revision)
        elif args.command=='verify':
            result=verify_paths(args.bundle,args.landing,args.work_root)
        elif args.command=='compare':
            result=compare(args.legacy_db,args.file_db,args.date,args.report)
        else:
            Store(os.environ['FILE_BATCH_SERVING_DSN']).finalization(args.date,args.revision,args.command=='finalize',args.reason)
            result={'status':args.command}
        print(json.dumps(result,ensure_ascii=False))
    except (BatchError,KeyError,ValueError) as exc:
        print(f'실패: {exc}',file=sys.stderr)
        return 1
    return 0


if __name__=='__main__':
    sys.exit(main())
