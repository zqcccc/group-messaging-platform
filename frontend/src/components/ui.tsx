import { useAuth } from '../auth';

// viewer 隐藏写操作：无权限时的占位说明
export function ReadOnlyNote() {
  const { isViewer } = useAuth();
  if (!isViewer) return null;
  return <div className="note">当前为 viewer 只读账号，写操作按钮已隐藏。</div>;
}
