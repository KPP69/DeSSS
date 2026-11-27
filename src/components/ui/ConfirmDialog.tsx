import React, { useState } from "react";
import Button from "./Button";
import Modal from "./Modal";

/** Unified second-confirmation dialog (never window.confirm). While
 * the confirm handler runs, both buttons are disabled (the confirm
 * button shows its loading spinner). */
export default function ConfirmDialog({
  open,
  title,
  message,
  confirmText = "确认删除",
  cancelText = "取消",
  onConfirm,
  onCancel,
}: {
  open: boolean;
  title: string;
  message: React.ReactNode;
  confirmText?: string;
  cancelText?: string;
  onConfirm: () => void | Promise<void>;
  onCancel: () => void;
}) {
  const [busy, setBusy] = useState(false);
  const confirm = async () => {
    setBusy(true);
    try {
      await onConfirm();
    } finally {
      setBusy(false);
    }
  };
  return (
    <Modal
      open={open}
      title={title}
      onClose={onCancel}
      size="sm"
      footer={
        <>
          <Button variant="secondary" onClick={onCancel} disabled={busy}>
            {cancelText}
          </Button>
          <Button variant="danger" onClick={confirm} loading={busy}>
            {confirmText}
          </Button>
        </>
      }
    >
      <div className="text-ds-text-2 pb-2">{message}</div>
    </Modal>
  );
}
