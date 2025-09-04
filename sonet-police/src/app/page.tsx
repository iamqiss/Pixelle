import Link from 'next/link'

export default function Home() {
  return (
    <main className="p-6">
      <div className="max-w-5xl mx-auto">
        <h1 className="text-3xl font-bold">Sonet Police</h1>
        <p className="text-gray-600 mt-2">Moderation dashboard</p>
        <div className="mt-6 flex gap-4">
          <Link className="px-4 py-2 rounded bg-black text-white" href="/queues">Open Queues</Link>
          <Link className="px-4 py-2 rounded bg-gray-900 text-white" href="/audit">View Audit</Link>
        </div>
      </div>
    </main>
  )
}

