"use client"
import React from 'react'

type Audit = {
  id: string
  action: string
  subject_id?: string
  actor_id?: string
  created_at: string
}

export default function AuditPage() {
  const [items, setItems] = React.useState<Audit[]>([])
  const [action, setAction] = React.useState<string>('')

  const fetchAudit = React.useCallback(async () => {
    const params = new URLSearchParams()
    if (action) params.set('action', action)
    params.set('limit', '100')
    const res = await fetch(`${process.env.NEXT_PUBLIC_MOD_API?.replace(/\/$/, '')}/api/v1/audit?${params.toString()}`, { credentials: 'include' })
    const json = await res.json()
    setItems(json.data || [])
  }, [action])

  React.useEffect(() => { fetchAudit() }, [fetchAudit])

  return (
    <main className="p-6">
      <div className="max-w-7xl mx-auto">
        <h1 className="text-2xl font-semibold mb-4">Audit</h1>
        <div className="flex gap-3 mb-4">
          <input value={action} onChange={e => setAction(e.target.value)} placeholder="Filter action" className="border rounded px-2 py-1" />
          <button className="px-3 py-1 rounded bg-black text-white" onClick={fetchAudit}>Refresh</button>
        </div>
        <div className="border rounded bg-white">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-left border-b">
                <th className="p-2">Time</th>
                <th className="p-2">Action</th>
                <th className="p-2">Subject</th>
                <th className="p-2">Actor</th>
              </tr>
            </thead>
            <tbody>
              {items.map(a => (
                <tr key={a.id} className="border-b hover:bg-gray-50">
                  <td className="p-2">{new Date(a.created_at).toLocaleString()}</td>
                  <td className="p-2">{a.action}</td>
                  <td className="p-2 font-mono text-xs">{a.subject_id}</td>
                  <td className="p-2 font-mono text-xs">{a.actor_id}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </main>
  )
}

