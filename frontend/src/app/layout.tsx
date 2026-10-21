import type { Metadata } from "next";
import "./globals.css";
import { Footer, ScopeStrip, TopNav } from "@/components/Shell";
import { ToastProvider } from "@/components/ui";

export const metadata: Metadata = {
  title: { default: "TrafficSense", template: "%s · TrafficSense" },
  description: "Urban traffic intelligence and congestion prediction with Apache Spark: analytics, live predictions, model evaluation and data management.",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>
        <ToastProvider>
          <TopNav />
          <ScopeStrip />
          <main className="mx-auto max-w-[1400px] px-4 py-8 sm:px-6">{children}</main>
          <Footer />
        </ToastProvider>
      </body>
    </html>
  );
}
