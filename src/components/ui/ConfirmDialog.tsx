import React from "react";
import Button from "./Button";
import Modal from "./Modal";

/** Unified second-confirmation dialog (never window.confirm). */
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
  onConfirm: () => void;
  onCancel: () => void;
}) {
  return (
    <Modal
      open={open}
      title={title}
      onClose={onCancel}
      widthClass="w-[420px]"
      footer={
        <>
          <Button variant="secondary" onClick={onCancel}>
            {cancelText}
          </Button>
          <Button variant="danger" onClick={onConfirm}>
            {confirmText}
          </Button>
        </>
      }
    >
      <div className="text-t2 pb-2">{message}</div>
    </Modal>
  );
}
