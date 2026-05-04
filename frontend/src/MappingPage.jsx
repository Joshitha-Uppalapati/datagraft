import { useState } from "react";
import axios from "axios";

function MappingPage({ fileId, onMappingConfirmed }) {
  const [schemaType, setSchemaType] = useState("contact");
  const [detected, setDetected] = useState(null);
  const [mappingResult, setMappingResult] = useState(null);

  const schemas = {
    contact: [
      { name: "first_name", type: "string", required: true, variants: [] },
      { name: "last_name", type: "string", required: false, variants: [] },
      { name: "email", type: "email", required: true, variants: [] },
      { name: "phone", type: "phone", required: true, variants: [] },
    ],
    transaction: [
      { name: "date", type: "date", required: true, variants: [] },
      { name: "amount", type: "float", required: true, variants: [] },
      { name: "description", type: "string", required: false, variants: [] },
    ],
  };

  const runDetection = async () => {
    const res = await axios.get(
      `http://localhost:8000/api/detect/${fileId}`
    );
    setDetected(res.data.columns);
  };

  const runMapping = async () => {
    const res = await axios.post(
      `http://localhost:8000/api/map/${fileId}`,
      { target_schema: schemas[schemaType] }
    );
    setMappingResult(res.data.mappings);
  };

  const confirmMapping = async () => {
    await axios.post(
      `http://localhost:8000/api/map/${fileId}/confirm`,
      { auto_confirm: true }
    );
    onMappingConfirmed();
  };

  return (
    <div>
      <h2>Mapping</h2>

      <div>
        <label>Schema:</label>
        <select
          value={schemaType}
          onChange={(e) => setSchemaType(e.target.value)}
        >
          <option value="contact">Contact List</option>
          <option value="transaction">Transaction Log</option>
        </select>
      </div>

      <button onClick={runDetection}>Run Detection</button>

      {detected && (
        <table border="1" cellPadding="5">
          <thead>
            <tr>
              <th>Column</th>
              <th>Type</th>
              <th>Confidence</th>
            </tr>
          </thead>
          <tbody>
            {detected.map((col, idx) => (
              <tr key={idx}>
                <td>{col.original_name}</td>
                <td>{col.inferred_type}</td>
                <td>{col.confidence}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      {detected && <button onClick={runMapping}>Run Mapping</button>}

      {mappingResult && (
        <div>
          <pre>{JSON.stringify(mappingResult, null, 2)}</pre>
          <button onClick={confirmMapping}>Confirm Mapping</button>
        </div>
      )}
    </div>
  );
}

export default MappingPage;