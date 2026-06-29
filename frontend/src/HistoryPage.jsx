import { useEffect, useState } from "react";
import api from "./api";

function HistoryPage({ onBack }) {
  const [imports, setImports] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    const loadHistory = async () => {
      setLoading(true);
      setError("");

      try {
        const res = await api.get("/api/history");
        setImports(res.data);
      } catch (err) {
        setError(err?.response?.data?.detail || "Failed to load history.");
      } finally {
        setLoading(false);
      }
    };

    loadHistory();
  }, []);

  if (loading) {
    return <p className="text-slate-600">Loading history...</p>;
  }

  if (error) {
    return (
      <div className="rounded border border-red-300 bg-red-50 p-4 text-red-700">
        {error}
      </div>
    );
  }

  return (
    <div>
      <div className="mb-6 flex items-center justify-between">
        <div>
          <h2 className="text-xl font-semibold">Import History</h2>
          <p className="mt-1 text-sm text-slate-500">
            Recent DataGraft import sessions.
          </p>
        </div>

        <button
          onClick={onBack}
          className="rounded border border-slate-300 px-4 py-2 text-sm text-slate-700"
        >
          Back to Upload
        </button>
      </div>

      {imports.length === 0 ? (
        <p className="text-slate-600">No imports yet.</p>
      ) : (
        <div className="overflow-x-auto rounded border border-slate-200">
          <table className="w-full border-collapse text-left text-sm">
            <thead className="border-b bg-slate-50">
              <tr>
                <th className="px-4 py-3">Filename</th>
                <th className="px-4 py-3">State</th>
                <th className="px-4 py-3">Rows</th>
                <th className="px-4 py-3">Clean</th>
                <th className="px-4 py-3">Errors</th>
                <th className="px-4 py-3">Created</th>
              </tr>
            </thead>

            <tbody>
              {imports.map((item) => {
                const summary = item.validation_summary || {};

                return (
                  <tr key={item.file_id} className="border-b last:border-b-0">
                    <td className="px-4 py-3 font-medium text-slate-900">
                      {item.filename}
                    </td>
                    <td className="px-4 py-3">
                      <span className="rounded-full bg-slate-100 px-2 py-1 text-xs font-medium text-slate-700">
                        {item.state}
                      </span>
                    </td>
                    <td className="px-4 py-3">{item.row_count}</td>
                    <td className="px-4 py-3 text-green-700">
                      {summary.clean_rows ?? "-"}
                    </td>
                    <td className="px-4 py-3 text-red-700">
                      {summary.error_rows ?? "-"}
                    </td>
                    <td className="px-4 py-3 text-slate-600">
                      {new Date(item.created_at).toLocaleString()}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

export default HistoryPage;