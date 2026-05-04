function ExportPage({ fileId, onRestart }) {
  const downloadUrl = `http://localhost:8000/api/export/${fileId}`;

  return (
    <div className="mx-auto max-w-xl rounded-xl border border-slate-200 bg-white p-8 text-center shadow-sm">
      <div className="mx-auto flex h-14 w-14 items-center justify-center rounded-full bg-green-100 text-2xl text-green-700">
        ✓
      </div>

      <h2 className="mt-4 text-2xl font-semibold text-slate-900">
        Export Ready
      </h2>

      <p className="mt-2 text-slate-600">
        Your data has been cleaned and is ready for download.
      </p>

      <div className="mt-6 flex justify-center gap-3">
        <a
          href={downloadUrl}
          className="rounded bg-slate-900 px-4 py-2 text-white"
        >
          Download Clean CSV
        </a>

        <button
          type="button"
          onClick={onRestart}
          className="rounded border border-slate-300 px-4 py-2 text-slate-700"
        >
          Start New Import
        </button>
      </div>
    </div>
  );
}

export default ExportPage;